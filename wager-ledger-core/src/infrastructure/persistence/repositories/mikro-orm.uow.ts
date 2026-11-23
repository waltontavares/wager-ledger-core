import { Injectable } from '@nestjs/common';
import { EntityManager, LockMode } from '@mikro-orm/postgresql';
import {
  LedgerCursor,
  TransactionContext,
  UnitOfWorkPort,
} from '../../../application/ports/application.ports.js';
import { Wallet, WalletStatus } from '../../../domain/entities/wallet.entity.js';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../../domain/entities/wager-transaction.entity.js';
import {
  LedgerEntryDirection,
  WalletLedgerEntry,
} from '../../../domain/entities/wallet-ledger-entry.entity.js';
import {
  InboxMessage,
  IntegrationEventEnvelope,
  OutboxMessage,
} from '../../../domain/entities/messaging.entities.js';
import { Money } from '../../../domain/value-objects/money.vo.js';
import { DomainError, LedgerFailureCode } from '../../../domain/errors/ledger.errors.js';
import {
  InboxMessageOrmEntity,
  OutboxMessageOrmEntity,
  WagerTransactionOrmEntity,
  WalletLedgerEntryOrmEntity,
  WalletOrmEntity,
} from '../entities/orm.entities.js';

export function encodeLedgerCursor(cursor: LedgerCursor): string {
  return Buffer.from(JSON.stringify(cursor), 'utf8').toString('base64url');
}

export function decodeLedgerCursor(raw: string): LedgerCursor | null {
  try {
    const parsed = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8'));
    if (
      parsed &&
      typeof parsed.createdAt === 'string' &&
      typeof parsed.id === 'string' &&
      !Number.isNaN(Date.parse(parsed.createdAt))
    ) {
      return { createdAt: parsed.createdAt, id: parsed.id };
    }
    return null;
  } catch {
    return null;
  }
}

@Injectable()
export class MikroOrmUnitOfWork implements UnitOfWorkPort {
  constructor(private readonly em: EntityManager) {}

  async runInTransaction<T>(work: (ctx: TransactionContext) => Promise<T>): Promise<T> {
    return this.em.transactional(async (txEm) => {
      const ctx: TransactionContext = {
        wallets: {
          findByIdForUpdate: async (id: string) => {
            const orm = await txEm.findOne(
              WalletOrmEntity,
              { id },
              { lockMode: LockMode.PESSIMISTIC_WRITE },
            );
            return orm ? mapWalletOrmToDomain(orm) : null;
          },
          findById: async (id: string) => {
            const orm = await txEm.findOne(WalletOrmEntity, { id });
            return orm ? mapWalletOrmToDomain(orm) : null;
          },
          findByPlayerAndCurrency: async (playerId: string, currency: string) => {
            const orm = await txEm.findOne(WalletOrmEntity, {
              playerId,
              currency: currency.toUpperCase(),
            });
            return orm ? mapWalletOrmToDomain(orm) : null;
          },
          insert: async (wallet: Wallet) => {
            try {
              const orm = txEm.create(WalletOrmEntity, {
                id: wallet.id,
                playerId: wallet.playerId,
                currency: wallet.balance.currency,
                balanceAmount: wallet.balance.amount,
                status: wallet.status,
                version: wallet.version,
                createdAt: wallet.createdAt,
                updatedAt: wallet.updatedAt,
              });
              await txEm.persist(orm).flush();
            } catch (err: unknown) {
              const pgCode = (err as { code?: string })?.code;
              if (pgCode === '23505') {
                throw new DomainError(
                  LedgerFailureCode.WALLET_ALREADY_EXISTS,
                  `Wallet already exists for player '${wallet.playerId}' and currency '${wallet.balance.currency}'`,
                );
              }
              throw err;
            }
          },
          updateWithVersionCheck: async (wallet: Wallet, expectedVersion: number) => {
            const affected = await txEm.nativeUpdate(
              WalletOrmEntity,
              { id: wallet.id, version: expectedVersion },
              {
                balanceAmount: wallet.balance.amount,
                status: wallet.status,
                version: wallet.version,
                updatedAt: wallet.updatedAt,
              },
            );
            if (affected === 0) {
              throw new DomainError(
                LedgerFailureCode.INVALID_TRANSACTION_STATE,
                `Optimistic lock conflict updating wallet ${wallet.id} at version ${expectedVersion}`,
                true,
              );
            }
          },
        },

        transactions: {
          findById: async (id: string) => {
            const orm = await txEm.findOne(WagerTransactionOrmEntity, { id });
            return orm ? mapTxOrmToDomain(orm) : null;
          },
          findByIdempotencyKey: async (providerId: string, idempotencyKey: string) => {
            const orm = await txEm.findOne(WagerTransactionOrmEntity, {
              providerId,
              idempotencyKey,
            });
            return orm ? mapTxOrmToDomain(orm) : null;
          },
          findByExternalId: async (providerId: string, externalTransactionId: string) => {
            const orm = await txEm.findOne(WagerTransactionOrmEntity, {
              providerId,
              externalTransactionId,
            });
            return orm ? mapTxOrmToDomain(orm) : null;
          },
          findProcessedReversalByReference: async (
            providerId: string,
            referenceExternalTransactionId: string,
            kinds: Array<WagerTransactionKind>,
          ) => {
            const orm = await txEm.findOne(WagerTransactionOrmEntity, {
              providerId,
              referenceExternalTransactionId,
              kind: { $in: kinds },
              status: WagerTransactionStatus.PROCESSED,
            });
            return orm ? mapTxOrmToDomain(orm) : null;
          },
          findDuePendingReferences: async (limit: number) => {
            const now = new Date();
            const list = await txEm.find(
              WagerTransactionOrmEntity,
              {
                status: WagerTransactionStatus.PENDING_REFERENCE,
                nextRetryAt: { $lte: now },
              },
              { limit, orderBy: { nextRetryAt: 'ASC', createdAt: 'ASC' } },
            );
            return list.map((orm) => ({
              transaction: mapTxOrmToDomain(orm),
              idempotencyKey: orm.idempotencyKey,
            }));
          },
          save: async (tx: WagerTransaction, idempotencyKey: string) => {
            await txEm.upsert(WagerTransactionOrmEntity, {
              id: tx.id,
              providerId: tx.providerId,
              idempotencyKey,
              externalTransactionId: tx.externalTransactionId,
              walletId: tx.walletId,
              kind: tx.kind,
              amount: tx.amount.amount,
              currency: tx.amount.currency,
              status: tx.status,
              payloadHash: tx.payloadHash,
              referenceExternalTransactionId: tx.referenceExternalTransactionId,
              roundId: tx.roundId,
              gameId: tx.gameId,
              failureCode: tx.failureCode,
              failureReason: tx.failureReason,
              retryAttempts: tx.retryAttempts,
              nextRetryAt: tx.nextRetryAt,
              createdAt: tx.createdAt,
              updatedAt: tx.updatedAt,
            });
          },
        },

        ledgerEntries: {
          save: async (entry: WalletLedgerEntry) => {
            const orm = txEm.create(WalletLedgerEntryOrmEntity, {
              id: entry.id,
              walletId: entry.walletId,
              transactionId: entry.transactionId,
              direction: entry.direction,
              amount: entry.amount.amount,
              balanceBefore: entry.balanceBefore.amount,
              balanceAfter: entry.balanceAfter.amount,
              currency: entry.amount.currency,
              createdAt: entry.createdAt,
            });
            await txEm.persist(orm).flush();
          },
          findByTransactionId: async (transactionId: string) => {
            const orm = await txEm.findOne(WalletLedgerEntryOrmEntity, { transactionId });
            return orm ? mapLedgerOrmToDomain(orm) : null;
          },
          findPaginatedByWalletId: async (
            walletId: string,
            limit: number,
            cursor?: LedgerCursor,
          ) => {
            const params: Array<unknown> = [walletId];
            let cursorClause = '';

            if (cursor) {
              cursorClause = `AND (created_at, id) < (?::timestamptz, ?::uuid)`;
              params.push(cursor.createdAt, cursor.id);
            }

            params.push(limit + 1);
            const limitParamIdx = params.length;

            const rows = await txEm.getConnection().execute<Array<{
              id: string;
              wallet_id: string;
              transaction_id: string;
              direction: string;
              amount: string;
              balance_before: string;
              balance_after: string;
              currency: string;
              created_at: string | Date;
            }>>(
              `SELECT id, wallet_id, transaction_id, direction, amount::text, balance_before::text, balance_after::text, currency, created_at
               FROM wallet_ledger_entries
               WHERE wallet_id = ?
                 ${cursorClause}
               ORDER BY created_at DESC, id DESC
               LIMIT ?`,
              params,
            );

            const hasMore = rows.length > limit;
            const pageRows = hasMore ? rows.slice(0, limit) : rows;

            const items = pageRows.map((row) =>
              WalletLedgerEntry.rehydrate({
                id: row.id,
                walletId: row.wallet_id,
                transactionId: row.transaction_id,
                direction: row.direction as LedgerEntryDirection,
                amount: Money.fromContract(row.amount, row.currency),
                balanceBefore: Money.fromContract(row.balance_before, row.currency),
                balanceAfter: Money.fromContract(row.balance_after, row.currency),
                createdAt: new Date(row.created_at),
              }),
            );

            const lastItem = items[items.length - 1];
            const nextCursor =
              hasMore && lastItem
                ? encodeLedgerCursor({
                    createdAt: lastItem.createdAt.toISOString(),
                    id: lastItem.id,
                  })
                : null;

            return { items, nextCursor };
          },
          reconcileSummaryByWalletId: async (walletId: string) => {
            const rows = await txEm.getConnection().execute<Array<{
              total: string | null;
              checked_entries: string | number;
            }>>(
              `SELECT
                 COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0)::text AS total,
                 COUNT(*)::int AS checked_entries
               FROM wallet_ledger_entries
               WHERE wallet_id = ?`,
              [walletId],
            );
            const first = rows[0];
            return {
              sumAmount: first?.total ?? '0.00',
              checkedEntries: Number(first?.checked_entries ?? 0),
            };
          },
        },

        inbox: {
          tryAcquire: async (msg: InboxMessage) => {
            const exists = await txEm.findOne(InboxMessageOrmEntity, {
              consumerName: msg.consumerName,
              messageId: msg.messageId,
            });
            if (exists) return false;
            const orm = txEm.create(InboxMessageOrmEntity, {
              consumerName: msg.consumerName,
              messageId: msg.messageId,
              receivedAt: msg.receivedAt,
              processedAt: msg.processedAt,
            });
            await txEm.persist(orm).flush();
            return true;
          },
          markProcessed: async (consumerName: string, messageId: string) => {
            await txEm.nativeUpdate(
              InboxMessageOrmEntity,
              { consumerName, messageId },
              { processedAt: new Date() },
            );
          },
        },

        outbox: {
          save: async (msg: OutboxMessage) => {
            const orm = txEm.create(OutboxMessageOrmEntity, {
              id: msg.id,
              eventType: msg.envelope.eventType,
              aggregateId: msg.envelope.aggregateId,
              correlationId: msg.envelope.correlationId,
              causationId: msg.envelope.causationId,
              occurredAt: new Date(msg.envelope.occurredAt),
              version: msg.envelope.version,
              envelope: msg.envelope as unknown as Record<string, unknown>,
              attempts: msg.attempts,
              nextAttemptAt: msg.nextAttemptAt,
              publishedAt: msg.publishedAt,
              createdAt: msg.createdAt,
            });
            await txEm.persist(orm).flush();
          },
          claimPendingBatchForUpdate: async (limit: number) => {
            const rows = await txEm.getConnection().execute<Array<{
              id: string;
              envelope: IntegrationEventEnvelope;
              attempts: number;
              next_attempt_at: string | Date;
              published_at: string | Date | null;
              created_at: string | Date;
            }>>(
              `SELECT id, envelope, attempts, next_attempt_at, published_at, created_at
               FROM outbox_messages
               WHERE published_at IS NULL
                 AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
               ORDER BY next_attempt_at ASC NULLS FIRST, occurred_at ASC
               LIMIT ?
               FOR UPDATE SKIP LOCKED`,
              [limit],
            );

            return rows.map((row) =>
              OutboxMessage.rehydrate({
                id: row.id,
                envelope: typeof row.envelope === 'string' ? JSON.parse(row.envelope) : row.envelope,
                attempts: Number(row.attempts),
                nextAttemptAt: new Date(row.next_attempt_at),
                publishedAt: row.published_at ? new Date(row.published_at) : null,
                createdAt: new Date(row.created_at),
              }),
            );
          },
          markPublished: async (id: string) => {
            await txEm.nativeUpdate(OutboxMessageOrmEntity, { id }, { publishedAt: new Date() });
          },
          recordFailure: async (id: string, attempts: number, nextAttemptAt: Date) => {
            await txEm.nativeUpdate(OutboxMessageOrmEntity, { id }, { attempts, nextAttemptAt });
          },
        },
      };

      return work(ctx);
    });
  }
}

function mapWalletOrmToDomain(orm: WalletOrmEntity): Wallet {
  return Wallet.rehydrate({
    id: orm.id,
    playerId: orm.playerId,
    balance: Money.fromContract(orm.balanceAmount, orm.currency),
    status: orm.status as WalletStatus,
    version: orm.version,
    createdAt: orm.createdAt,
    updatedAt: orm.updatedAt,
  });
}

function mapTxOrmToDomain(orm: WagerTransactionOrmEntity): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: orm.id,
    providerId: orm.providerId,
    externalTransactionId: orm.externalTransactionId,
    walletId: orm.walletId,
    kind: orm.kind as WagerTransactionKind,
    amount: Money.fromContract(orm.amount, orm.currency),
    status: orm.status as WagerTransactionStatus,
    payloadHash: orm.payloadHash,
    referenceExternalTransactionId: orm.referenceExternalTransactionId,
    roundId: orm.roundId,
    gameId: orm.gameId,
    failureCode: orm.failureCode,
    failureReason: orm.failureReason,
    retryAttempts: orm.retryAttempts,
    nextRetryAt: orm.nextRetryAt,
    createdAt: orm.createdAt,
    updatedAt: orm.updatedAt,
  });
}

function mapLedgerOrmToDomain(orm: WalletLedgerEntryOrmEntity): WalletLedgerEntry {
  return WalletLedgerEntry.rehydrate({
    id: orm.id,
    walletId: orm.walletId,
    transactionId: orm.transactionId,
    direction: orm.direction as LedgerEntryDirection,
    amount: Money.fromContract(orm.amount, orm.currency),
    balanceBefore: Money.fromContract(orm.balanceBefore, orm.currency),
    balanceAfter: Money.fromContract(orm.balanceAfter, orm.currency),
    createdAt: orm.createdAt,
  });
}
