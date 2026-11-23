import { Money } from '../value-objects/money.vo.js';

export enum WagerTransactionType {
  BET = 'BET',
  WIN = 'WIN',
  REFUND = 'REFUND',
}

export enum WagerTransactionStatus {
  COMPLETED = 'COMPLETED',
  PENDING_REFERENCE = 'PENDING_REFERENCE',
  FAILED = 'FAILED',
}

export class WagerTransaction {
  constructor(
    public readonly id: string,
    public readonly externalTransactionId: string,
    public readonly providerId: string,
    public readonly walletId: string,
    public readonly type: WagerTransactionType,
    public readonly amount: Money,
    public status: WagerTransactionStatus,
    public readonly referenceTransactionId?: string,
    public failureCode?: string,
    public readonly createdAt: Date = new Date(),
  ) {}
}

export enum LedgerEntryDirection {
  DEBIT = 'DEBIT',
  CREDIT = 'CREDIT',
}

export class WalletLedgerEntry {
  constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerEntryDirection,
    public readonly amount: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date = new Date(),
  ) {}
}

export class InboxMessage {
  constructor(
    public readonly messageId: string,
    public readonly source: string,
    public readonly payload: Record<string, unknown>,
    public status: 'RECEIVED' | 'PROCESSED' | 'FAILED',
    public readonly receivedAt: Date = new Date(),
  ) {}
}

export class OutboxMessage {
  constructor(
    public readonly id: string,
    public readonly aggregateType: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Record<string, unknown>,
    public status: 'PENDING' | 'PUBLISHED' | 'FAILED' = 'PENDING',
    public retryCount: number = 0,
    public readonly createdAt: Date = new Date(),
  ) {}
}
