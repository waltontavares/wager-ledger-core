import {
  BeforeApplicationShutdown,
  Injectable,
  OnModuleInit,
} from '@nestjs/common';
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  SendMessageCommand,
  ChangeMessageVisibilityCommand,
  Message,
} from '@aws-sdk/client-sqs';
import {
  computeExponentialBackoffWithJitter,
  ProcessWagerTransactionUseCase,
  ReplayPendingReferencesUseCase,
} from '../../application/use-cases/ledger.use-cases.js';
import {
  LoggerPort,
  MetricsPort,
  UnitOfWorkPort,
} from '../../application/ports/application.ports.js';
import { DomainError } from '../../domain/errors/ledger.errors.js';

interface InFlightMessageContext {
  messageId: string;
  receiptHandle: string;
}

@Injectable()
export class LedgerBackgroundWorkers implements OnModuleInit, BeforeApplicationShutdown {
  private readonly sqs: SQSClient;
  private readonly queueUrl: string;
  private readonly dlqUrl: string;
  private readonly outboxQueueUrl: string;
  private readonly maxReceiveCount = Number(process.env.SQS_MAX_RECEIVE_COUNT ?? 3);
  private readonly shutdownGracePeriodMs = Number(process.env.SHUTDOWN_GRACE_PERIOD_MS ?? 15_000);
  private readonly outboxBaseBackoffMs = Number(process.env.OUTBOX_BASE_BACKOFF_MS ?? 1_000);
  private readonly outboxMaxBackoffMs = Number(process.env.OUTBOX_MAX_BACKOFF_MS ?? 60_000);

  private shuttingDown = false;
  private outboxTimer?: ReturnType<typeof setInterval>;
  private reaperTimer?: ReturnType<typeof setInterval>;
  private readonly inFlightTasks = new Set<Promise<void>>();
  private readonly activeMessages = new Map<string, InFlightMessageContext>();

  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly processWager: ProcessWagerTransactionUseCase,
    private readonly replayPending: ReplayPendingReferencesUseCase,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {
    this.queueUrl =
      process.env.SQS_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
    this.dlqUrl =
      process.env.SQS_DLQ_URL ?? 'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
    this.outboxQueueUrl =
      process.env.SQS_OUTBOX_QUEUE_URL ?? this.queueUrl;

    this.sqs = new SQSClient({
      region: process.env.AWS_REGION ?? 'us-east-1',
      endpoint: process.env.AWS_ENDPOINT ?? 'http://localhost:4566',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      },
    });
  }

  onModuleInit(): void {
    this.shuttingDown = false;
    void this.pollSqsLoop();

    this.outboxTimer = setInterval(() => {
      if (!this.shuttingDown) {
        this.trackTask(this.flushOutbox().then(() => undefined));
      }
    }, 1500);

    this.reaperTimer = setInterval(() => {
      if (!this.shuttingDown) {
        this.trackTask(this.replayPending.execute(50).then(() => undefined));
      }
    }, 2000);
  }

  async beforeApplicationShutdown(signal?: string): Promise<void> {
    this.logger.info('Graceful shutdown initiated', {
      signal: signal ?? 'UNKNOWN',
      inFlightCount: this.inFlightTasks.size,
    });

    this.shuttingDown = true;
    if (this.outboxTimer) clearInterval(this.outboxTimer);
    if (this.reaperTimer) clearInterval(this.reaperTimer);

    if (this.inFlightTasks.size > 0) {
      const drainPromise = Promise.allSettled(Array.from(this.inFlightTasks));
      const timeoutPromise = new Promise<'TIMEOUT'>((resolve) =>
        setTimeout(() => resolve('TIMEOUT'), this.shutdownGracePeriodMs),
      );

      const outcome = await Promise.race([drainPromise, timeoutPromise]);

      if (outcome === 'TIMEOUT' && this.activeMessages.size > 0) {
        this.logger.warn('Shutdown grace period expired; releasing in-flight visibility', {
          remainingCount: this.activeMessages.size,
        });
        await Promise.allSettled(
          Array.from(this.activeMessages.values()).map((ctx) =>
            this.changeVisibility(ctx.receiptHandle, 0),
          ),
        );
      }
    }

    this.sqs.destroy();
    this.logger.info('Background workers closed cleanly');
  }

  private trackTask(task: Promise<void>): void {
    this.inFlightTasks.add(task);
    void task.finally(() => {
      this.inFlightTasks.delete(task);
    });
  }

  private async pollSqsLoop(): Promise<void> {
    while (!this.shuttingDown) {
      try {
        const res = await this.sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: this.queueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 5,
            MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
          }),
        );

        if (this.shuttingDown) {
          for (const msg of res.Messages ?? new Array<Message>()) {
            if (msg.ReceiptHandle) {
              await this.changeVisibility(msg.ReceiptHandle, 0);
            }
          }
          break;
        }

        for (const msg of res.Messages ?? new Array<Message>()) {
          if (!msg.Body || !msg.ReceiptHandle || !msg.MessageId) continue;

          const task = this.handleSingleMessage(msg);
          this.trackTask(task);
          await task;
        }
      } catch (err) {
        if (this.shuttingDown) break;
        this.logger.error('SQS polling error', err);
        await new Promise((r) => setTimeout(r, 2000));
      }
    }
  }

  private async handleSingleMessage(msg: Message): Promise<void> {
    const messageId = msg.MessageId!;
    const receiptHandle = msg.ReceiptHandle!;
    const receiveCount = Number(msg.Attributes?.ApproximateReceiveCount ?? '1');

    this.activeMessages.set(messageId, { messageId, receiptHandle });

    let correlationId = messageId;
    let providerId: string | undefined;
    let walletId: string | undefined;

    try {
      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(msg.Body!);
      } catch {
        this.metrics.incrementCounter('sqs_dlq_messages_total', { code: 'MALFORMED_JSON' });
        await this.sendToDlq(msg, 'MALFORMED_JSON', 'Malformed JSON payload');
        await this.deleteMessage(receiptHandle);
        return;
      }

      if (parsed.eventType && parsed.eventId) {
        await this.deleteMessage(receiptHandle);
        return;
      }

      correlationId = parsed.correlationId ? String(parsed.correlationId) : messageId;
      providerId = parsed.providerId ? String(parsed.providerId) : undefined;
      walletId = parsed.walletId ? String(parsed.walletId) : undefined;

      await this.processWager.execute({
        providerId: providerId ?? '',
        idempotencyKey: String(parsed.idempotencyKey ?? messageId),
        externalTransactionId: String(parsed.externalTransactionId ?? ''),
        walletId: walletId ?? '',
        kind: parsed.kind as never,
        amount: String(parsed.amount ?? ''),
        currency: String(parsed.currency ?? ''),
        referenceExternalTransactionId: parsed.referenceExternalTransactionId
          ? String(parsed.referenceExternalTransactionId)
          : undefined,
        roundId: parsed.roundId ? String(parsed.roundId) : undefined,
        gameId: parsed.gameId ? String(parsed.gameId) : undefined,
        correlationId,
        inbox: {
          consumerName: 'wager-sqs-consumer',
          messageId,
        },
      });

      await this.deleteMessage(receiptHandle);
    } catch (err) {
      const pgCode = (err as { code?: string })?.code ?? '';
      if (pgCode === '40001' || pgCode === '40P01' || pgCode === '55P03') {
        this.metrics.incrementCounter('wager_lock_conflicts_total', { source: 'sqs' });
      }

      if (err instanceof DomainError && !err.retryable) {
        this.metrics.incrementCounter('sqs_dlq_messages_total', { code: err.code });
        this.logger.warn('Permanent domain validation error on SQS message; routing to DLQ', {
          correlationId,
          messageId,
          providerId,
          walletId,
          code: err.code,
        });
        await this.sendToDlq(msg, err.code, err.message);
        await this.deleteMessage(receiptHandle);
        return;
      }

      if (receiveCount >= this.maxReceiveCount) {
        this.metrics.incrementCounter('sqs_dlq_messages_total', { code: 'MAX_RETRIES_EXCEEDED' });
        this.logger.error('Max receive count exceeded; routing message to DLQ', err, {
          correlationId,
          messageId,
          providerId,
          walletId,
          receiveCount,
        });
        await this.sendToDlq(
          msg,
          'MAX_RETRIES_EXCEEDED',
          err instanceof Error ? err.message : String(err),
        );
        await this.deleteMessage(receiptHandle);
        return;
      }

      this.metrics.incrementCounter('sqs_retries_total');
      const backoffSec = Math.max(
        1,
        Math.round(computeExponentialBackoffWithJitter(receiveCount - 1, 2_000, 30_000) / 1000),
      );
      this.logger.warn('Transient error on SQS message; deferring visibility', {
        correlationId,
        messageId,
        providerId,
        walletId,
        receiveCount,
        visibilityTimeoutSec: backoffSec,
      });
      await this.changeVisibility(receiptHandle, backoffSec);
    } finally {
      this.activeMessages.delete(messageId);
    }
  }

  private async sendToDlq(msg: Message, failureCode: string, reason: string): Promise<void> {
    const groupId = msg.Attributes?.MessageGroupId ?? 'dlq-group';
    const dlqEnvelope = {
      originalMessageId: msg.MessageId,
      failureCode,
      reason,
      failedAt: new Date().toISOString(),
      rawBody: msg.Body,
    };

    await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: this.dlqUrl,
        MessageBody: JSON.stringify(dlqEnvelope),
        MessageGroupId: groupId,
        MessageDeduplicationId: `${msg.MessageId}-${failureCode}`,
      }),
    );
  }

  private async changeVisibility(receiptHandle: string, timeoutSeconds: number): Promise<void> {
    try {
      await this.sqs.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: this.queueUrl,
          ReceiptHandle: receiptHandle,
          VisibilityTimeout: timeoutSeconds,
        }),
      );
    } catch (err) {
      this.logger.warn('Failed to change SQS message visibility', { timeoutSeconds });
    }
  }

  private async deleteMessage(receiptHandle: string): Promise<void> {
    await this.sqs.send(
      new DeleteMessageCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  async flushOutbox(batchSize = 50): Promise<number> {
    return this.uow.runInTransaction(async (ctx) => {
      const batch = await ctx.outbox.claimPendingBatchForUpdate(batchSize);
      let publishedCount = 0;

      for (const item of batch) {
        try {
          await this.sqs.send(
            new SendMessageCommand({
              QueueUrl: this.outboxQueueUrl,
              MessageBody: JSON.stringify(item.envelope),
              MessageGroupId: item.envelope.aggregateId,
              MessageDeduplicationId: item.envelope.eventId,
              MessageAttributes: {
                eventId: { DataType: 'String', StringValue: item.envelope.eventId },
                eventType: { DataType: 'String', StringValue: item.envelope.eventType },
                correlationId: { DataType: 'String', StringValue: item.envelope.correlationId },
              },
            }),
          );

          item.markPublished();
          await ctx.outbox.markPublished(item.id);
          this.metrics.incrementCounter('outbox_published_total', {
            eventType: item.envelope.eventType,
          });
          publishedCount++;
        } catch (err) {
          const backoffMs = computeExponentialBackoffWithJitter(
            item.attempts,
            this.outboxBaseBackoffMs,
            this.outboxMaxBackoffMs,
          );
          item.recordAttemptFailure(backoffMs);
          await ctx.outbox.recordFailure(item.id, item.attempts, item.nextAttemptAt);
          this.metrics.incrementCounter('outbox_retries_total');

          this.logger.error('Outbox publish failed; scheduled retry', err, {
            correlationId: item.envelope.correlationId,
            walletId: item.envelope.aggregateId,
            outboxId: item.id,
            eventId: item.envelope.eventId,
            attempts: item.attempts,
          });
        }
      }

      return publishedCount;
    });
  }
}
