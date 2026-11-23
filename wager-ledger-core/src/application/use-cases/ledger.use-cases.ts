import { randomUUID } from 'node:crypto';
import { Decimal } from 'decimal.js';
import { Wallet } from '../../domain/entities/wallet.entity.js';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/entities/wager-transaction.entity.js';
import { InboxMessage, OutboxMessage } from '../../domain/entities/messaging.entities.js';
import { IntegrationEvent } from '../../domain/events/integration.events.js';
import { Money, MoneyProps } from '../../domain/value-objects/money.vo.js';
import { DomainError, LedgerFailureCode } from '../../domain/errors/ledger.errors.js';
import {
  LoggerPort,
  MetricsPort,
  TransactionContext,
  UnitOfWorkPort,
} from '../ports/application.ports.js';

export interface ProcessWagerCommand {
  providerId: string;
  idempotencyKey: string;
  externalTransactionId: string;
  walletId: string;
  kind: WagerTransactionKind;
  amount: string;
  currency: string;
  referenceExternalTransactionId?: string;
  roundId?: string;
  gameId?: string;
  correlationId?: string;
  isReaperReplay?: boolean;
  inbox?: {
    consumerName: string;
    messageId: string;
  };
}

export interface ProcessWagerResult {
  transaction: WagerTransaction;
  observedBalance: MoneyProps;
  idempotentReplay: boolean;
  inboxDuplicate?: boolean;
}

export interface PendingReferencePolicy {
  maxAttempts: number;
  ttlMs: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

export const DEFAULT_PENDING_REFERENCE_POLICY: PendingReferencePolicy = {
  maxAttempts: Number(process.env.PENDING_REF_MAX_ATTEMPTS ?? 5),
  ttlMs: Number(process.env.PENDING_REF_TTL_MS ?? 300_000),
  baseDelayMs: Number(process.env.PENDING_REF_BASE_DELAY_MS ?? 2_000),
  maxDelayMs: Number(process.env.PENDING_REF_MAX_DELAY_MS ?? 60_000),
};

export function computeExponentialBackoffWithJitter(
  attempt: number,
  baseDelayMs: number,
  maxDelayMs: number,
): number {
  const exponential = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
  const jitterFactor = 0.5 + Math.random() * 0.5;
  return Math.round(exponential * jitterFactor);
}

const REJECTABLE_CODES = new Set<LedgerFailureCode>([
  LedgerFailureCode.INSUFFICIENT_FUNDS,
  LedgerFailureCode.ROLLBACK_WOULD_OVERDRAFT,
  LedgerFailureCode.WALLET_SUSPENDED,
  LedgerFailureCode.CURRENCY_MISMATCH,
  LedgerFailureCode.REFERENCE_NOT_FOUND,
  LedgerFailureCode.INVALID_REFERENCE_TARGET,
  LedgerFailureCode.REFERENCE_CONTEXT_MISMATCH,
  LedgerFailureCode.REFERENCE_AMOUNT_MISMATCH,
  LedgerFailureCode.TRANSACTION_ALREADY_REVERSED,
]);

export class ProcessWagerTransactionUseCase {
  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
    private readonly policy: PendingReferencePolicy = DEFAULT_PENDING_REFERENCE_POLICY,
  ) {}

  async execute(cmd: ProcessWagerCommand): Promise<ProcessWagerResult> {
    const start = Date.now();
    const money = Money.fromContract(cmd.amount, cmd.currency);

    const candidateTx = WagerTransaction.create({
      providerId: cmd.providerId,
      externalTransactionId: cmd.externalTransactionId,
      walletId: cmd.walletId,
      kind: cmd.kind,
      amount: money,
      referenceExternalTransactionId: cmd.referenceExternalTransactionId,
      roundId: cmd.roundId,
      gameId: cmd.gameId,
    });

    // ÚNICA TRANSAÇÃO SQL ATÔMICA (Inbox + Idempotência + Wallet + WagerTransaction + LedgerEntry + Outbox)
    return this.uow.runInTransaction(async (ctx) => {
      // 0. Deduplicação atômica de mensagem SQS (InboxMessage dentro da mesma transação)
      if (cmd.inbox) {
        const inboxMsg = InboxMessage.receive(cmd.inbox.consumerName, cmd.inbox.messageId);
        inboxMsg.markProcessed();
        const acquired = await ctx.inbox.tryAcquire(inboxMsg);
        if (!acquired) {
          this.metrics.incrementCounter('wager.inbox_duplicate', {
            consumer: cmd.inbox.consumerName,
          });
          return {
            transaction: candidateTx,
            observedBalance: Money.zero(cmd.currency).toProps(),
            idempotentReplay: true,
            inboxDuplicate: true,
          };
        }
      }

      // 1. Idempotência por (providerId, idempotencyKey)
      const existingByKey = await ctx.transactions.findByIdempotencyKey(
        cmd.providerId,
        cmd.idempotencyKey,
      );

      let txToProcess: WagerTransaction = candidateTx;

      if (existingByKey) {
        if (existingByKey.payloadHash !== candidateTx.payloadHash) {
          throw new DomainError(
            LedgerFailureCode.IDEMPOTENCY_PAYLOAD_MISMATCH,
            `Idempotency key '${cmd.idempotencyKey}' was already used with a different payload`,
          );
        }

        if (
          !cmd.isReaperReplay ||
          existingByKey.status !== WagerTransactionStatus.PENDING_REFERENCE
        ) {
          const ledgerEntry = await ctx.ledgerEntries.findByTransactionId(existingByKey.id);
          let observedBalance: MoneyProps;

          if (ledgerEntry) {
            observedBalance = ledgerEntry.balanceAfter.toProps();
          } else {
            const currentWallet = await ctx.wallets.findById(existingByKey.walletId);
            observedBalance = currentWallet
              ? currentWallet.balance.toProps()
              : Money.zero(existingByKey.amount.currency).toProps();
          }

          this.metrics.incrementCounter('wager.idempotent_hit', {
            kind: existingByKey.kind,
            status: existingByKey.status,
          });

          return {
            transaction: existingByKey,
            observedBalance,
            idempotentReplay: true,
          };
        }

        txToProcess = existingByKey;
      }

      // 2. Lock pessimista na linha da walletId (SELECT ... FOR UPDATE)
      const wallet = await ctx.wallets.findByIdForUpdate(cmd.walletId);
      if (!wallet) {
        throw new DomainError(LedgerFailureCode.WALLET_NOT_FOUND, `Wallet ${cmd.walletId} not found`);
      }

      const expectedVersion = wallet.version;
      const balanceBefore = wallet.balance;

      // 3. Resolução e validação de transação referenciada
      let refTx: WagerTransaction | null = null;
      if (cmd.referenceExternalTransactionId) {
        refTx = await ctx.transactions.findByExternalId(
          cmd.providerId,
          cmd.referenceExternalTransactionId,
        );

        if (!refTx) {
          const ageMs = Date.now() - txToProcess.createdAt.getTime();
          const nextAttemptNumber =
            txToProcess.status === WagerTransactionStatus.PENDING_REFERENCE
              ? txToProcess.retryAttempts + 1
              : 0;

          const exhausted =
            nextAttemptNumber >= this.policy.maxAttempts || ageMs >= this.policy.ttlMs;

          if (exhausted) {
            const reason = `Referenced transaction '${cmd.referenceExternalTransactionId}' not found after ${nextAttemptNumber} attempts / ${ageMs}ms TTL`;
            txToProcess.markRejected(LedgerFailureCode.REFERENCE_NOT_FOUND, reason);
            await ctx.transactions.save(txToProcess, cmd.idempotencyKey);
            await this.enqueueRejectedEvent(ctx, txToProcess, wallet, cmd.correlationId);
            this.metrics.incrementCounter('wager.pending_reference_exhausted', {
              kind: txToProcess.kind,
            });
            return {
              transaction: txToProcess,
              observedBalance: wallet.balance.toProps(),
              idempotentReplay: false,
            };
          }

          const delayMs = computeExponentialBackoffWithJitter(
            nextAttemptNumber,
            this.policy.baseDelayMs,
            this.policy.maxDelayMs,
          );
          const nextRetryAt = new Date(Date.now() + delayMs);

          if (txToProcess.status === WagerTransactionStatus.PENDING) {
            txToProcess.markPendingReference(nextRetryAt);
            await ctx.transactions.save(txToProcess, cmd.idempotencyKey);
            await this.enqueuePendingReferenceEvent(
              ctx,
              txToProcess,
              wallet,
              nextRetryAt,
              cmd.correlationId,
            );
            this.metrics.incrementCounter('wager.pending_reference', { kind: txToProcess.kind });
          } else {
            txToProcess.scheduleNextReferenceRetry(nextRetryAt);
            await ctx.transactions.save(txToProcess, cmd.idempotencyKey);
            this.metrics.incrementCounter('wager.pending_reference_rescheduled', {
              kind: txToProcess.kind,
            });
          }

          return {
            transaction: txToProcess,
            observedBalance: wallet.balance.toProps(),
            idempotentReplay: false,
          };
        }
      }

      // 4. Execução das regras específicas por kind
      try {
        let entry = null;

        switch (cmd.kind) {
          case WagerTransactionKind.BET: {
            entry = wallet.debit(money, txToProcess.id);
            break;
          }

          case WagerTransactionKind.WIN: {
            if (refTx) {
              this.assertSameContext(txToProcess, refTx);
              if (refTx.status !== WagerTransactionStatus.PROCESSED) {
                throw new DomainError(
                  LedgerFailureCode.INVALID_REFERENCE_TARGET,
                  `Referenced transaction '${refTx.externalTransactionId}' is not PROCESSED (status: ${refTx.status})`,
                );
              }
            }
            entry = wallet.credit(money, txToProcess.id);
            break;
          }

          case WagerTransactionKind.LOSS: {
            wallet.debit(Money.zero(money.currency), txToProcess.id);
            break;
          }

          case WagerTransactionKind.REFUND: {
            if (!refTx) {
              throw new DomainError(
                LedgerFailureCode.MISSING_REFERENCE_TRANSACTION,
                'REFUND requires a valid referenceExternalTransactionId',
              );
            }
            this.assertSameContext(txToProcess, refTx);

            if (
              refTx.kind !== WagerTransactionKind.BET ||
              refTx.status !== WagerTransactionStatus.PROCESSED
            ) {
              throw new DomainError(
                LedgerFailureCode.INVALID_REFERENCE_TARGET,
                `REFUND can only target a PROCESSED BET (found kind=${refTx.kind}, status=${refTx.status})`,
              );
            }

            if (!refTx.amount.equals(money)) {
              throw new DomainError(
                LedgerFailureCode.REFERENCE_AMOUNT_MISMATCH,
                `REFUND amount (${money.amount}) must match original BET amount (${refTx.amount.amount})`,
              );
            }

            const alreadyReversed = await ctx.transactions.findProcessedReversalByReference(
              cmd.providerId,
              refTx.externalTransactionId,
              [WagerTransactionKind.REFUND, WagerTransactionKind.ROLLBACK],
            );
            if (alreadyReversed) {
              throw new DomainError(
                LedgerFailureCode.TRANSACTION_ALREADY_REVERSED,
                `Transaction '${refTx.externalTransactionId}' was already reversed by '${alreadyReversed.externalTransactionId}' (${alreadyReversed.kind})`,
              );
            }

            entry = wallet.credit(money, txToProcess.id);
            break;
          }

          case WagerTransactionKind.ROLLBACK: {
            if (!refTx) {
              throw new DomainError(
                LedgerFailureCode.MISSING_REFERENCE_TRANSACTION,
                'ROLLBACK requires a valid referenceExternalTransactionId',
              );
            }
            this.assertSameContext(txToProcess, refTx);

            const allowedTargets = new Set<WagerTransactionKind>([
              WagerTransactionKind.BET,
              WagerTransactionKind.WIN,
              WagerTransactionKind.REFUND,
            ]);

            if (
              !allowedTargets.has(refTx.kind) ||
              refTx.status !== WagerTransactionStatus.PROCESSED
            ) {
              throw new DomainError(
                LedgerFailureCode.INVALID_REFERENCE_TARGET,
                `ROLLBACK can only target a PROCESSED BET, WIN or REFUND (found kind=${refTx.kind}, status=${refTx.status})`,
              );
            }

            if (!refTx.amount.equals(money)) {
              throw new DomainError(
                LedgerFailureCode.REFERENCE_AMOUNT_MISMATCH,
                `ROLLBACK amount (${money.amount}) must match original transaction amount (${refTx.amount.amount})`,
              );
            }

            const reversalKindsToCheck =
              refTx.kind === WagerTransactionKind.BET
                ? [WagerTransactionKind.REFUND, WagerTransactionKind.ROLLBACK]
                : [WagerTransactionKind.ROLLBACK];

            const alreadyReversed = await ctx.transactions.findProcessedReversalByReference(
              cmd.providerId,
              refTx.externalTransactionId,
              reversalKindsToCheck,
            );
            if (alreadyReversed) {
              throw new DomainError(
                LedgerFailureCode.TRANSACTION_ALREADY_REVERSED,
                `Transaction '${refTx.externalTransactionId}' was already reversed by '${alreadyReversed.externalTransactionId}' (${alreadyReversed.kind})`,
              );
            }

            if (refTx.kind === WagerTransactionKind.BET) {
              entry = wallet.credit(money, txToProcess.id);
            } else {
              try {
                entry = wallet.debit(money, txToProcess.id);
              } catch (debitErr) {
                if (
                  debitErr instanceof DomainError &&
                  debitErr.code === LedgerFailureCode.INSUFFICIENT_FUNDS
                ) {
                  throw new DomainError(
                    LedgerFailureCode.ROLLBACK_WOULD_OVERDRAFT,
                    `Rollback debit of ${money.amount} ${money.currency} would result in negative balance (available: ${wallet.balance.amount})`,
                  );
                }
                throw debitErr;
              }
            }
            break;
          }

          default:
            throw new DomainError(
              LedgerFailureCode.INVALID_TRANSACTION_KIND,
              `Unsupported transaction kind: ${cmd.kind}`,
            );
        }

        txToProcess.markProcessed();

        if (wallet.version !== expectedVersion) {
          await ctx.wallets.updateWithVersionCheck(wallet, expectedVersion);
        }

        await ctx.transactions.save(txToProcess, cmd.idempotencyKey);

        if (entry) {
          await ctx.ledgerEntries.save(entry);
        }

        await this.enqueueOutboxEvents(
          ctx,
          txToProcess,
          wallet,
          balanceBefore,
          entry !== null,
          cmd.correlationId,
        );

        this.metrics.incrementCounter("wager_transactions_total", { status: txToProcess.status, kind: cmd.kind });
        this.metrics.observeHistogram("wager_process_duration_ms", Date.now() - start, { kind: cmd.kind });
        this.logger.info("Wager transaction processed", {
          correlationId: cmd.correlationId ?? txToProcess.id,
          transactionId: txToProcess.id,
          providerId: txToProcess.providerId,
          walletId: wallet.id,
          kind: txToProcess.kind,
          status: txToProcess.status,
        });

        return {
          transaction: txToProcess,
          observedBalance: wallet.balance.toProps(),
          idempotentReplay: false,
        };
      } catch (err) {
        if (err instanceof DomainError && REJECTABLE_CODES.has(err.code)) {
          txToProcess.markRejected(err.code, err.message);
          await ctx.transactions.save(txToProcess, cmd.idempotencyKey);
          await this.enqueueRejectedEvent(ctx, txToProcess, wallet, cmd.correlationId);
          this.metrics.incrementCounter("wager_transactions_total", { status: txToProcess.status, kind: cmd.kind });
          this.metrics.incrementCounter("wager_rejected_total", { kind: cmd.kind, code: err.code });
          this.logger.warn("Wager transaction rejected", {
            correlationId: cmd.correlationId ?? txToProcess.id,
            transactionId: txToProcess.id,
            providerId: txToProcess.providerId,
            walletId: wallet.id,
            kind: cmd.kind,
            status: txToProcess.status,
            failureCode: err.code,
          });
          return {
            transaction: txToProcess,
            observedBalance: wallet.balance.toProps(),
            idempotentReplay: false,
          };
        }
        throw err;
      }
    });
  }

  private assertSameContext(current: WagerTransaction, ref: WagerTransaction): void {
    if (
      current.providerId !== ref.providerId ||
      current.walletId !== ref.walletId ||
      current.amount.currency !== ref.amount.currency ||
      (current.roundId ?? null) !== (ref.roundId ?? null)
    ) {
      throw new DomainError(
        LedgerFailureCode.REFERENCE_CONTEXT_MISMATCH,
        `Context mismatch between '${current.externalTransactionId}' and referenced '${ref.externalTransactionId}' (provider, wallet, currency or roundId differs)`,
      );
    }
  }

  private async enqueuePendingReferenceEvent(
    ctx: TransactionContext,
    tx: WagerTransaction,
    wallet: Wallet,
    nextRetryAt: Date,
    correlationId?: string,
  ): Promise<void> {
    const event = IntegrationEvent.create({
      eventType: 'WagerTransactionPendingReference',
      aggregateId: wallet.id,
      correlationId: correlationId ?? tx.id,
      causationId: tx.id,
      version: 1,
      data: {
        transactionId: tx.id,
        externalTransactionId: tx.externalTransactionId,
        providerId: tx.providerId,
        walletId: wallet.id,
        playerId: wallet.playerId,
        kind: tx.kind,
        status: tx.status,
        amount: tx.amount.toProps(),
        referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
        nextRetryAt: nextRetryAt.toISOString(),
      },
    });
    await ctx.outbox.save(OutboxMessage.create(event));
  }

  private async enqueueRejectedEvent(
    ctx: TransactionContext,
    tx: WagerTransaction,
    wallet: Wallet,
    correlationId?: string,
  ): Promise<void> {
    const event = IntegrationEvent.create({
      eventType: 'WagerTransactionRejected',
      aggregateId: wallet.id,
      correlationId: correlationId ?? tx.id,
      causationId: tx.id,
      version: 1,
      data: {
        transactionId: tx.id,
        externalTransactionId: tx.externalTransactionId,
        providerId: tx.providerId,
        walletId: wallet.id,
        playerId: wallet.playerId,
        kind: tx.kind,
        status: tx.status,
        failureCode: tx.failureCode ?? null,
        failureReason: tx.failureReason ?? null,
        amount: tx.amount.toProps(),
        referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
      },
    });
    await ctx.outbox.save(OutboxMessage.create(event));
  }

  private async enqueueOutboxEvents(
    ctx: TransactionContext,
    tx: WagerTransaction,
    wallet: Wallet,
    balanceBefore: Money,
    balanceChanged: boolean,
    correlationId?: string,
  ): Promise<void> {
    const corrId = correlationId ?? tx.id;
    const nowIso = new Date().toISOString();

    const processedEvent = IntegrationEvent.create({
      eventType: 'WagerTransactionProcessed',
      aggregateId: wallet.id,
      correlationId: corrId,
      causationId: tx.id,
      occurredAt: nowIso,
      version: 1,
      data: {
        transactionId: tx.id,
        externalTransactionId: tx.externalTransactionId,
        providerId: tx.providerId,
        walletId: wallet.id,
        playerId: wallet.playerId,
        kind: tx.kind,
        status: tx.status,
        amount: tx.amount.toProps(),
        balanceAfter: wallet.balance.toProps(),
        referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
        roundId: tx.roundId ?? null,
      },
    });
    await ctx.outbox.save(OutboxMessage.create(processedEvent));

    if (balanceChanged) {
      const balanceEvent = IntegrationEvent.create({
        eventType: 'WalletBalanceChanged',
        aggregateId: wallet.id,
        correlationId: corrId,
        causationId: tx.id,
        occurredAt: nowIso,
        version: wallet.version,
        data: {
          walletId: wallet.id,
          playerId: wallet.playerId,
          transactionId: tx.id,
          kind: tx.kind,
          balanceBefore: balanceBefore.toProps(),
          balanceAfter: wallet.balance.toProps(),
          walletVersion: wallet.version,
        },
      });
      await ctx.outbox.save(OutboxMessage.create(balanceEvent));
    }
  }
}

export class OpenWalletUseCase {
  constructor(private readonly uow: UnitOfWorkPort) {}

  async execute(
    playerId: string,
    currency: string,
    initialAmount?: string,
  ): Promise<Wallet> {
    if (!playerId || playerId.trim() === "") {
      throw new DomainError(LedgerFailureCode.INVALID_MONEY_FORMAT, "playerId is required");
    }

    const normalizedPlayerId = playerId.trim();
    const initialBalance = initialAmount !== undefined
      ? Money.fromContract(initialAmount, currency)
      : Money.zero(currency);

    return this.uow.runInTransaction(async (ctx) => {
      const existing = await ctx.wallets.findByPlayerAndCurrency(
        normalizedPlayerId,
        initialBalance.currency,
      );
      if (existing) {
        throw new DomainError(
          LedgerFailureCode.WALLET_ALREADY_EXISTS,
          `Wallet already exists for player '${normalizedPlayerId}' and currency '${initialBalance.currency}'`,
        );
      }

      const openingTxId = initialBalance.isGreaterThanZero() ? randomUUID() : undefined;
      const { wallet, openingEntry } = Wallet.open({
        playerId: normalizedPlayerId,
        currency: initialBalance.currency,
        initialBalance,
        openingTransactionId: openingTxId,
      });

      await ctx.wallets.insert(wallet);

      if (openingEntry && openingTxId) {
        const openingTx = WagerTransaction.create({
          id: openingTxId,
          providerId: "SYSTEM",
          externalTransactionId: `OPENING-${wallet.id}`,
          walletId: wallet.id,
          kind: WagerTransactionKind.OPENING,
          amount: initialBalance,
          allowOpening: true,
        });
        openingTx.markProcessed();
        await ctx.transactions.save(openingTx, `OPENING-${wallet.id}`);
        await ctx.ledgerEntries.save(openingEntry);
      }

      return wallet;
    });
  }
}

export interface WalletReconciliationReport {
  walletId: string;
  currency: string;
  walletBalance: string;
  ledgerSum: string;
  consistent: boolean;
  difference: string;
  checkedEntries: number;
}

export class ReconcileWalletUseCase {
  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {}

  async execute(walletId: string): Promise<WalletReconciliationReport> {
    return this.uow.runInTransaction(async (ctx) => {
      const wallet = await ctx.wallets.findById(walletId);
      if (!wallet) {
        throw new DomainError(LedgerFailureCode.WALLET_NOT_FOUND, `Wallet ${walletId} not found`);
      }

      const summary = await ctx.ledgerEntries.reconcileSummaryByWalletId(walletId);
      const walletBalDec = new Decimal(wallet.balance.amount);
      const ledgerSumDec = new Decimal(summary.sumAmount);
      const diffDec = walletBalDec.minus(ledgerSumDec);
      const consistent = diffDec.isZero();

      if (!consistent) {
        this.metrics.incrementCounter("wallet.reconciliation.divergence", {
          currency: wallet.balance.currency,
        });
        this.logger.error("CRITICAL: Wallet balance divergence detected during reconciliation", null, {
          walletId: wallet.id,
          playerId: wallet.playerId,
          currency: wallet.balance.currency,
          walletBalance: walletBalDec.toFixed(2),
          ledgerSum: ledgerSumDec.toFixed(2),
          difference: diffDec.toFixed(2),
          checkedEntries: summary.checkedEntries,
        });
      } else {
        this.metrics.incrementCounter("wallet.reconciliation.ok", {
          currency: wallet.balance.currency,
        });
      }

      return {
        walletId: wallet.id,
        currency: wallet.balance.currency,
        walletBalance: walletBalDec.toFixed(2),
        ledgerSum: ledgerSumDec.toFixed(2),
        consistent,
        difference: diffDec.toFixed(2),
        checkedEntries: summary.checkedEntries,
      };
    });
  }
}

export class ReplayPendingReferencesUseCase {
  constructor(
    private readonly uow: UnitOfWorkPort,
    private readonly processWager: ProcessWagerTransactionUseCase,
    private readonly logger: LoggerPort,
  ) {}

  async execute(batchSize = 50): Promise<number> {
    const dueRecords = await this.uow.runInTransaction((ctx) =>
      ctx.transactions.findDuePendingReferences(batchSize),
    );

    let replayed = 0;
    for (const { transaction: tx, idempotencyKey } of dueRecords) {
      try {
        await this.processWager.execute({
          providerId: tx.providerId,
          idempotencyKey,
          externalTransactionId: tx.externalTransactionId,
          walletId: tx.walletId,
          kind: tx.kind,
          amount: tx.amount.amount,
          currency: tx.amount.currency,
          referenceExternalTransactionId: tx.referenceExternalTransactionId,
          roundId: tx.roundId,
          gameId: tx.gameId,
          isReaperReplay: true,
        });
        replayed++;
      } catch (err) {
        this.logger.warn("Failed to replay pending reference tx", { txId: tx.id, err });
      }
    }
    return replayed;
  }
}
