import { createHash, randomUUID } from 'node:crypto';
import { Money } from '../value-objects/money.vo.js';
import {
  DomainError,
  InvalidTransactionStateError,
  LedgerFailureCode,
} from '../errors/ledger.errors.js';

export enum WagerTransactionKind {
  OPENING = 'OPENING',
  BET = 'BET',
  WIN = 'WIN',
  LOSS = 'LOSS',
  REFUND = 'REFUND',
  ROLLBACK = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  PENDING = 'PENDING',
  PENDING_REFERENCE = 'PENDING_REFERENCE',
  PROCESSED = 'PROCESSED',
  REJECTED = 'REJECTED',
  FAILED = 'FAILED',
}

const TERMINAL_STATES = new Set<WagerTransactionStatus>([
  WagerTransactionStatus.PROCESSED,
  WagerTransactionStatus.REJECTED,
  WagerTransactionStatus.FAILED,
]);

export interface CreateWagerTransactionProps {
  id?: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  kind: WagerTransactionKind;
  amount: Money;
  referenceExternalTransactionId?: string;
  roundId?: string;
  gameId?: string;
  allowOpening?: boolean;
}

export interface RehydrateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  kind: WagerTransactionKind;
  amount: Money;
  status: WagerTransactionStatus;
  payloadHash: string;
  referenceExternalTransactionId?: string;
  roundId?: string;
  gameId?: string;
  failureCode?: string;
  failureReason?: string;
  retryAttempts?: number;
  nextRetryAt?: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

export function canonicalJsonStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJsonStringify(item)).join(',')}]`;
  }
  const record = value as Record<string, unknown>;
  const sortedKeys = Object.keys(record)
    .filter((k) => record[k] !== undefined)
    .sort();
  const entries = sortedKeys.map(
    (k) => `${JSON.stringify(k)}:${canonicalJsonStringify(record[k])}`,
  );
  return `{${entries.join(',')}}`;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly walletId: string,
    public readonly kind: WagerTransactionKind,
    public readonly amount: Money,
    private _status: WagerTransactionStatus,
    public readonly payloadHash: string,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly roundId: string | undefined,
    public readonly gameId: string | undefined,
    private _failureCode: string | undefined,
    private _failureReason: string | undefined,
    private _retryAttempts: number,
    private _nextRetryAt: Date | null,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static computePayloadHash(businessPayload: {
    providerId: string;
    externalTransactionId: string;
    walletId: string;
    kind: WagerTransactionKind;
    amount: string;
    currency: string;
    referenceExternalTransactionId?: string;
    roundId?: string;
    gameId?: string;
  }): string {
    const canonical = canonicalJsonStringify(businessPayload);
    return createHash('sha256').update(canonical, 'utf8').digest('hex');
  }

  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.OPENING && !props.allowOpening) {
      throw new DomainError(
        LedgerFailureCode.INVALID_TRANSACTION_KIND,
        'OPENING transactions are forbidden on external API/queue contracts',
      );
    }

    if (
      (props.kind === WagerTransactionKind.REFUND ||
        props.kind === WagerTransactionKind.ROLLBACK) &&
      (!props.referenceExternalTransactionId ||
        props.referenceExternalTransactionId.trim() === '')
    ) {
      throw new DomainError(
        LedgerFailureCode.MISSING_REFERENCE_TRANSACTION,
        `Transaction kind '${props.kind}' requires a non-empty referenceExternalTransactionId`,
      );
    }

    const payloadHash = WagerTransaction.computePayloadHash({
      providerId: props.providerId,
      externalTransactionId: props.externalTransactionId,
      walletId: props.walletId,
      kind: props.kind,
      amount: props.amount.amount,
      currency: props.amount.currency,
      referenceExternalTransactionId: props.referenceExternalTransactionId,
      roundId: props.roundId,
      gameId: props.gameId,
    });

    const now = new Date();
    return new WagerTransaction(
      props.id ?? randomUUID(),
      props.providerId,
      props.externalTransactionId,
      props.walletId,
      props.kind,
      props.amount,
      WagerTransactionStatus.PENDING,
      payloadHash,
      props.referenceExternalTransactionId,
      props.roundId,
      props.gameId,
      undefined,
      undefined,
      0,
      null,
      now,
      now,
    );
  }

  static rehydrate(props: RehydrateWagerTransactionProps): WagerTransaction {
    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.walletId,
      props.kind,
      props.amount,
      props.status,
      props.payloadHash,
      props.referenceExternalTransactionId,
      props.roundId,
      props.gameId,
      props.failureCode,
      props.failureReason,
      props.retryAttempts ?? 0,
      props.nextRetryAt ?? null,
      props.createdAt,
      props.updatedAt,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get failureCode(): string | undefined {
    return this._failureCode;
  }

  get failureReason(): string | undefined {
    return this._failureReason;
  }

  get retryAttempts(): number {
    return this._retryAttempts;
  }

  get nextRetryAt(): Date | null {
    return this._nextRetryAt;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  get isTerminal(): boolean {
    return TERMINAL_STATES.has(this._status);
  }

  markPendingReference(nextRetryAt: Date): void {
    this.assertNotTerminal(WagerTransactionStatus.PENDING_REFERENCE);
    this._status = WagerTransactionStatus.PENDING_REFERENCE;
    this._nextRetryAt = nextRetryAt;
    this._updatedAt = new Date();
  }

  scheduleNextReferenceRetry(nextRetryAt: Date): void {
    if (this._status !== WagerTransactionStatus.PENDING_REFERENCE) {
      throw new InvalidTransactionStateError(this._status, 'SCHEDULE_RETRY');
    }
    this._retryAttempts += 1;
    this._nextRetryAt = nextRetryAt;
    this._updatedAt = new Date();
  }

  markProcessed(): void {
    this.assertNotTerminal(WagerTransactionStatus.PROCESSED);
    this._status = WagerTransactionStatus.PROCESSED;
    this._nextRetryAt = null;
    this._updatedAt = new Date();
  }

  markRejected(code: string, reason: string): void {
    this.assertNotTerminal(WagerTransactionStatus.REJECTED);
    this._status = WagerTransactionStatus.REJECTED;
    this._failureCode = code;
    this._failureReason = reason;
    this._nextRetryAt = null;
    this._updatedAt = new Date();
  }

  markFailed(code: string, reason: string): void {
    this.assertNotTerminal(WagerTransactionStatus.FAILED);
    this._status = WagerTransactionStatus.FAILED;
    this._failureCode = code;
    this._failureReason = reason;
    this._nextRetryAt = null;
    this._updatedAt = new Date();
  }

  private assertNotTerminal(target: WagerTransactionStatus): void {
    if (this.isTerminal) {
      throw new InvalidTransactionStateError(this._status, target);
    }
  }
}
