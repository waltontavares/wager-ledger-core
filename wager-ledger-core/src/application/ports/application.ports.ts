import { Wallet } from '../../domain/entities/wallet.entity.js';
import {
  WagerTransaction,
  WagerTransactionKind,
} from '../../domain/entities/wager-transaction.entity.js';
import { WalletLedgerEntry } from '../../domain/entities/wallet-ledger-entry.entity.js';
import { InboxMessage, OutboxMessage } from '../../domain/entities/messaging.entities.js';

export interface PendingReferenceRecord {
  transaction: WagerTransaction;
  idempotencyKey: string;
}

export interface LedgerCursor {
  createdAt: string;
  id: string;
}

export interface PaginatedLedgerResult {
  items: Array<WalletLedgerEntry>;
  nextCursor: string | null;
}

export interface LedgerReconciliationSummary {
  sumAmount: string;
  checkedEntries: number;
}

export interface WalletRepositoryPort {
  findByIdForUpdate(id: string): Promise<Wallet | null>;
  findById(id: string): Promise<Wallet | null>;
  findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | null>;
  insert(wallet: Wallet): Promise<void>;
  updateWithVersionCheck(wallet: Wallet, expectedVersion: number): Promise<void>;
}

export interface WagerTransactionRepositoryPort {
  findById(id: string): Promise<WagerTransaction | null>;
  findByIdempotencyKey(providerId: string, idempotencyKey: string): Promise<WagerTransaction | null>;
  findByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  findProcessedReversalByReference(
    providerId: string,
    referenceExternalTransactionId: string,
    kinds: Array<WagerTransactionKind>,
  ): Promise<WagerTransaction | null>;
  findDuePendingReferences(limit: number): Promise<Array<PendingReferenceRecord>>;
  save(tx: WagerTransaction, idempotencyKey: string): Promise<void>;
}

export interface LedgerEntryRepositoryPort {
  save(entry: WalletLedgerEntry): Promise<void>;
  findByTransactionId(transactionId: string): Promise<WalletLedgerEntry | null>;
  findPaginatedByWalletId(
    walletId: string,
    limit: number,
    cursor?: LedgerCursor,
  ): Promise<PaginatedLedgerResult>;
  reconcileSummaryByWalletId(walletId: string): Promise<LedgerReconciliationSummary>;
}

export interface InboxRepositoryPort {
  tryAcquire(message: InboxMessage): Promise<boolean>;
  markProcessed(consumerName: string, messageId: string): Promise<void>;
}

export interface OutboxRepositoryPort {
  save(message: OutboxMessage): Promise<void>;
  claimPendingBatchForUpdate(limit: number): Promise<Array<OutboxMessage>>;
  markPublished(id: string): Promise<void>;
  recordFailure(id: string, attempts: number, nextAttemptAt: Date): Promise<void>;
}

export interface TransactionContext {
  wallets: WalletRepositoryPort;
  transactions: WagerTransactionRepositoryPort;
  ledgerEntries: LedgerEntryRepositoryPort;
  inbox: InboxRepositoryPort;
  outbox: OutboxRepositoryPort;
}

export const UNIT_OF_WORK_PORT = Symbol('UNIT_OF_WORK_PORT');
export interface UnitOfWorkPort {
  runInTransaction<T>(work: (ctx: TransactionContext) => Promise<T>): Promise<T>;
}

export const LOGGER_PORT = Symbol('LOGGER_PORT');
export interface LoggerPort {
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, error?: unknown, meta?: Record<string, unknown>): void;
}

export const METRICS_PORT = Symbol('METRICS_PORT');
export interface MetricsPort {
  incrementCounter(name: string, tags?: Record<string, string>): void;
  observeHistogram(name: string, valueMs: number, tags?: Record<string, string>): void;
}
