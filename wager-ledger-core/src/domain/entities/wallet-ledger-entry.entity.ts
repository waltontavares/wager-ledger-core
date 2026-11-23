import { randomUUID } from 'node:crypto';
import { Money } from '../value-objects/money.vo.js';
import { DomainError, LedgerFailureCode } from '../errors/ledger.errors.js';

export enum LedgerEntryDirection {
  DEBIT = 'DEBIT',
  CREDIT = 'CREDIT',
}

export interface CreateLedgerEntryProps {
  id?: string;
  walletId: string;
  transactionId: string;
  direction: LedgerEntryDirection;
  amount: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt?: Date;
}

export class WalletLedgerEntry {
  public readonly id: string;
  public readonly walletId: string;
  public readonly transactionId: string;
  public readonly direction: LedgerEntryDirection;
  public readonly amount: Money;
  public readonly balanceBefore: Money;
  public readonly balanceAfter: Money;
  public readonly createdAt: Date;

  private constructor(props: Required<CreateLedgerEntryProps>) {
    this.id = props.id;
    this.walletId = props.walletId;
    this.transactionId = props.transactionId;
    this.direction = props.direction;
    this.amount = props.amount;
    this.balanceBefore = props.balanceBefore;
    this.balanceAfter = props.balanceAfter;
    this.createdAt = props.createdAt;
    Object.freeze(this);
  }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.amount.isGreaterThanZero()) {
      throw new DomainError(
        LedgerFailureCode.INVALID_LEDGER_ENTRY_INVARIANT,
        'WalletLedgerEntry amount must be strictly greater than 0.00',
      );
    }

    const expectedAfter =
      props.direction === LedgerEntryDirection.CREDIT
        ? props.balanceBefore.add(props.amount)
        : props.balanceBefore.subtract(props.amount);

    if (!expectedAfter.equals(props.balanceAfter)) {
      throw new DomainError(
        LedgerFailureCode.INVALID_LEDGER_ENTRY_INVARIANT,
        `Ledger invariant violated: ${props.balanceBefore.amount} ${props.direction} ${props.amount.amount} != ${props.balanceAfter.amount}`,
      );
    }

    return new WalletLedgerEntry({
      id: props.id ?? randomUUID(),
      walletId: props.walletId,
      transactionId: props.transactionId,
      direction: props.direction,
      amount: props.amount,
      balanceBefore: props.balanceBefore,
      balanceAfter: props.balanceAfter,
      createdAt: props.createdAt ?? new Date(),
    });
  }

  static rehydrate(props: Required<CreateLedgerEntryProps>): WalletLedgerEntry {
    return new WalletLedgerEntry(props);
  }
}
