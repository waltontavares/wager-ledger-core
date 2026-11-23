import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpException,
  HttpStatus,
  Inject,
  Param,
  Post,
  Query,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import type { Response } from 'express';
import { EntityManager } from '@mikro-orm/postgresql';
import { GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { PrometheusMetricsRegistry } from '../../observability/observability.service.js';
import { METRICS_PORT } from '../../../application/ports/application.ports.js';
import {
  OpenWalletUseCase,
  ProcessWagerTransactionUseCase,
  ReconcileWalletUseCase,
} from '../../../application/use-cases/ledger.use-cases.js';
import {
  UNIT_OF_WORK_PORT,
  UnitOfWorkPort,
} from '../../../application/ports/application.ports.js';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../../domain/entities/wager-transaction.entity.js';
import { DomainError, LedgerFailureCode } from '../../../domain/errors/ledger.errors.js';
import { ExplicitProviderIdentityGuard } from '../../security/explicit-provider-identity.guard.js';
import { decodeLedgerCursor } from '../../persistence/repositories/mikro-orm.uow.js';

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

@Controller()
export class LedgerController {
  private readonly sqs: SQSClient;
  private readonly queueUrl: string;

  constructor(
    private readonly openWallet: OpenWalletUseCase,
    private readonly processWager: ProcessWagerTransactionUseCase,
    private readonly reconcileWallet: ReconcileWalletUseCase,
    @Inject(UNIT_OF_WORK_PORT)
    private readonly uow: UnitOfWorkPort,
    @Inject(METRICS_PORT)
    private readonly metrics: PrometheusMetricsRegistry,
    private readonly em: EntityManager,
  ) {
    this.queueUrl =
      process.env.SQS_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
    this.sqs = new SQSClient({
      region: process.env.AWS_REGION ?? 'us-east-1',
      endpoint: process.env.AWS_ENDPOINT ?? 'http://localhost:4566',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      },
    });
  }

  // ==================== HEALTH CHECKS ====================

  @Get('health/live')
  live() {
    return {
      status: 'alive',
      timestamp: new Date().toISOString(),
    };
  }

  @Get('health/ready')
  async ready() {
    const checks: Record<string, string> = {};

    try {
      await this.em.getConnection().execute('SELECT 1');
      checks.postgres = 'up';
    } catch (err) {
      checks.postgres = 'down';
      throw new HttpException(
        { status: 'not_ready', checks, error: err instanceof Error ? err.message : String(err) },
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }

    try {
      await this.sqs.send(
        new GetQueueAttributesCommand({
          QueueUrl: this.queueUrl,
          AttributeNames: ['QueueArn'],
        }),
      );
      checks.sqs = 'up';
    } catch (err) {
      checks.sqs = 'down';
      throw new HttpException(
        { status: 'not_ready', checks, error: err instanceof Error ? err.message : String(err) },
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }

    return {
      status: 'ready',
      checks,
      timestamp: new Date().toISOString(),
    };
  }

  
  @Get('metrics')
  async getMetrics() {
    const outboxStats = await this.em.getConnection().execute<Array<{
      pending_count: number;
      oldest_lag_sec: number;
    }>>(
      `SELECT
         COUNT(*)::int AS pending_count,
         COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(occurred_at))), 0)::float AS oldest_lag_sec
       FROM outbox_messages
       WHERE published_at IS NULL`,
    );

    let dlqApproxMessages = 0;
    try {
      const dlqUrl =
        process.env.SQS_DLQ_URL ?? 'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
      const attrs = await this.sqs.send(
        new GetQueueAttributesCommand({
          QueueUrl: dlqUrl,
          AttributeNames: ['ApproximateNumberOfMessages'],
        }),
      );
      dlqApproxMessages = Number(attrs.Attributes?.ApproximateNumberOfMessages ?? 0);
    } catch {
      dlqApproxMessages = -1;
    }

    const row = outboxStats.at(0);
    return this.metrics.renderPrometheusText({
      outbox_pending_messages: Number(row?.pending_count ?? 0),
      outbox_oldest_lag_seconds: Number((row?.oldest_lag_sec ?? 0).toFixed(3)),
      sqs_dlq_approximate_messages: dlqApproxMessages,
    });
  }

  // ==================== WALLETS & LEDGER ====================

  @Post('wallets')
  @HttpCode(HttpStatus.CREATED)
  async createWallet(
    @Body() body: { playerId?: string; currency?: string; initialBalance?: string; initialAmount?: string },
  ) {
    if (!body?.playerId || typeof body.playerId !== 'string' || !body?.currency || typeof body.currency !== 'string') {
      throw new HttpException(
        { code: 'INVALID_REQUEST', message: 'playerId and currency are required' },
        HttpStatus.BAD_REQUEST,
      );
    }

    const initial = body.initialBalance ?? body.initialAmount;

    try {
      const wallet = await this.openWallet.execute(body.playerId, body.currency, initial);
      return {
        id: wallet.id,
        playerId: wallet.playerId,
        balance: wallet.balance.toProps(),
        status: wallet.status,
        version: wallet.version,
        createdAt: wallet.createdAt.toISOString(),
        updatedAt: wallet.updatedAt.toISOString(),
      };
    } catch (err) {
      throw this.mapErrorToHttp(err);
    }
  }

  @Get('wallets/:walletId')
  async getWallet(@Param('walletId') walletId: string) {
    this.assertUuid(walletId, 'walletId');

    const wallet = await this.uow.runInTransaction((ctx) => ctx.wallets.findById(walletId));
    if (!wallet) {
      throw new HttpException(
        { code: LedgerFailureCode.WALLET_NOT_FOUND, message: `Wallet '${walletId}' not found` },
        HttpStatus.NOT_FOUND,
      );
    }

    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toProps(),
      status: wallet.status,
      version: wallet.version,
      createdAt: wallet.createdAt.toISOString(),
      updatedAt: wallet.updatedAt.toISOString(),
    };
  }

  @Get('wallets/:walletId/ledger')
  async getWalletLedger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursorParam?: string,
    @Query('limit') limitParam?: string,
  ) {
    this.assertUuid(walletId, 'walletId');

    const limit = limitParam ? Number(limitParam) : 50;
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
      throw new HttpException(
        { code: 'INVALID_LIMIT', message: 'Query parameter limit must be an integer between 1 and 200' },
        HttpStatus.BAD_REQUEST,
      );
    }

    let decodedCursor = undefined;
    if (cursorParam !== undefined && cursorParam !== '') {
      const parsed = decodeLedgerCursor(cursorParam);
      if (!parsed) {
        throw new HttpException(
          { code: 'INVALID_CURSOR', message: 'Invalid opaque pagination cursor' },
          HttpStatus.BAD_REQUEST,
        );
      }
      decodedCursor = parsed;
    }

    const result = await this.uow.runInTransaction(async (ctx) => {
      const wallet = await ctx.wallets.findById(walletId);
      if (!wallet) {
        throw new DomainError(LedgerFailureCode.WALLET_NOT_FOUND, `Wallet '${walletId}' not found`);
      }
      return ctx.ledgerEntries.findPaginatedByWalletId(walletId, limit, decodedCursor);
    }).catch((err) => {
      throw this.mapErrorToHttp(err);
    });

    return {
      walletId,
      items: result.items.map((entry) => ({
        id: entry.id,
        walletId: entry.walletId,
        transactionId: entry.transactionId,
        direction: entry.direction,
        amount: entry.amount.toProps(),
        balanceBefore: entry.balanceBefore.toProps(),
        balanceAfter: entry.balanceAfter.toProps(),
        createdAt: entry.createdAt.toISOString(),
      })),
      nextCursor: result.nextCursor,
    };
  }

  @Post('wallets/:walletId/reconciliation')
  @HttpCode(HttpStatus.OK)
  async reconcile(@Param('walletId') walletId: string) {
    this.assertUuid(walletId, 'walletId');
    try {
      return await this.reconcileWallet.execute(walletId);
    } catch (err) {
      throw this.mapErrorToHttp(err);
    }
  }

  // ==================== WAGERING TRANSACTIONS ====================

  @Post('wagering/transactions')
  @UseGuards(ExplicitProviderIdentityGuard)
  async submitWager(
    @Req() req: { provider: { providerId: string } },
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Headers('x-correlation-id') correlationId: string | undefined,
    @Body()
    body: {
      externalTransactionId?: string;
      walletId?: string;
      kind?: WagerTransactionKind;
      amount?: string;
      currency?: string;
      referenceExternalTransactionId?: string;
      roundId?: string;
      gameId?: string;
    },
    @Res({ passthrough: true }) res: Response,
  ) {
    if (!idempotencyKey || idempotencyKey.trim() === '') {
      throw new HttpException(
        { code: 'MISSING_IDEMPOTENCY_KEY', message: 'Header Idempotency-Key is required' },
        HttpStatus.BAD_REQUEST,
      );
    }

    if (
      !body?.externalTransactionId ||
      !body?.walletId ||
      !body?.kind ||
      typeof body?.amount !== 'string' ||
      !body?.currency
    ) {
      throw new HttpException(
        {
          code: 'INVALID_REQUEST',
          message: 'externalTransactionId, walletId, kind, amount (string) and currency are required',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    this.assertUuid(body.walletId, 'walletId');

    if (!Object.values(WagerTransactionKind).includes(body.kind)) {
      throw new HttpException(
        { code: LedgerFailureCode.INVALID_TRANSACTION_KIND, message: `Invalid kind '${body.kind}'` },
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }

    try {
      const result = await this.processWager.execute({
        providerId: req.provider.providerId,
        idempotencyKey: idempotencyKey.trim(),
        externalTransactionId: body.externalTransactionId,
        walletId: body.walletId,
        kind: body.kind,
        amount: body.amount,
        currency: body.currency,
        referenceExternalTransactionId: body.referenceExternalTransactionId,
        roundId: body.roundId,
        gameId: body.gameId,
        correlationId,
      });

      if (result.idempotentReplay) {
        res.status(HttpStatus.OK);
      } else if (
        result.transaction.status === WagerTransactionStatus.PENDING ||
        result.transaction.status === WagerTransactionStatus.PENDING_REFERENCE
      ) {
        res.status(HttpStatus.ACCEPTED);
      } else {
        res.status(HttpStatus.CREATED);
      }

      return {
        ...this.formatTransaction(result.transaction),
        observedBalance: result.observedBalance,
        idempotentReplay: result.idempotentReplay,
      };
    } catch (err) {
      throw this.mapErrorToHttp(err);
    }
  }

  @Get('wagering/transactions/:transactionId')
  async getTransactionById(@Param('transactionId') transactionId: string) {
    this.assertUuid(transactionId, 'transactionId');

    const tx = await this.uow.runInTransaction((ctx) => ctx.transactions.findById(transactionId));
    if (!tx) {
      throw new HttpException(
        { code: 'TRANSACTION_NOT_FOUND', message: `Transaction '${transactionId}' not found` },
        HttpStatus.NOT_FOUND,
      );
    }

    return this.formatTransaction(tx);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getTransactionByProviderExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ) {
    const tx = await this.uow.runInTransaction((ctx) =>
      ctx.transactions.findByExternalId(providerId, externalTransactionId),
    );
    if (!tx) {
      throw new HttpException(
        {
          code: 'TRANSACTION_NOT_FOUND',
          message: `Transaction '${externalTransactionId}' for provider '${providerId}' not found`,
        },
        HttpStatus.NOT_FOUND,
      );
    }

    return this.formatTransaction(tx);
  }

  // ==================== HELPERS ====================

  private formatTransaction(tx: WagerTransaction) {
    return {
      transactionId: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      walletId: tx.walletId,
      kind: tx.kind,
      status: tx.status,
      amount: tx.amount.toProps(),
      referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
      roundId: tx.roundId ?? null,
      gameId: tx.gameId ?? null,
      failureCode: tx.failureCode ?? null,
      failureReason: tx.failureReason ?? null,
      createdAt: tx.createdAt.toISOString(),
      updatedAt: tx.updatedAt.toISOString(),
    };
  }

  private assertUuid(value: string, fieldName: string): void {
    if (!UUID_REGEX.test(value)) {
      throw new HttpException(
        { code: 'INVALID_UUID', message: `Parameter '${fieldName}' must be a valid UUID` },
        HttpStatus.BAD_REQUEST,
      );
    }
  }

  private mapErrorToHttp(err: unknown): HttpException {
    if (err instanceof HttpException) {
      return err;
    }

    if (err instanceof DomainError) {
      if (err.retryable) {
        return new HttpException(
          { code: err.code, message: err.message },
          HttpStatus.SERVICE_UNAVAILABLE,
        );
      }
      if (
        err.code === LedgerFailureCode.IDEMPOTENCY_PAYLOAD_MISMATCH ||
        err.code === LedgerFailureCode.WALLET_ALREADY_EXISTS
      ) {
        return new HttpException({ code: err.code, message: err.message }, HttpStatus.CONFLICT);
      }
      if (err.code === LedgerFailureCode.WALLET_NOT_FOUND) {
        return new HttpException({ code: err.code, message: err.message }, HttpStatus.NOT_FOUND);
      }
      return new HttpException(
        { code: err.code, message: err.message },
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }

    // Falhas transitórias de infraestrutura (ex.: conexão PostgreSQL recusada, deadlock, timeout) -> 503
    const pgCode = (err as { code?: string })?.code ?? '';
    if (pgCode === '40001' || pgCode === '40P01' || pgCode === '55P03') {
      this.metrics.incrementCounter('wager_lock_conflicts_total', { source: 'http' });
    }
    if (
      pgCode.startsWith('08') ||
      pgCode === '40001' ||
      pgCode === '40P01' ||
      pgCode === '55P03' ||
      pgCode === '57P01' ||
      pgCode === 'ECONNREFUSED'
    ) {
      return new HttpException(
        { code: 'TRANSIENT_STORAGE_UNAVAILABLE', message: 'Temporary database unavailability; please retry' },
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }

    return new HttpException(
      { code: 'INTERNAL_SERVER_ERROR', message: 'Unexpected internal error' },
      HttpStatus.INTERNAL_SERVER_ERROR,
    );
  }
}
