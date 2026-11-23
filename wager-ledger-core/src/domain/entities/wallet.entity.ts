import { randomUUID } from 'node:crypto';
import { Money } from '../value-objects/money.vo.js';
import { DomainError, LedgerFailureCode } from '../errors/ledger.errors.js';
import { LedgerEntryDirection, WalletLedgerEntry } from './wallet-ledger-entry.entity.js';

export enum WalletStatus {
  ACTIVE = 'ACTIVE',
  SUSPENDED = 'SUSPENDED',
}

export interface OpenWalletProps {
  id?: string;
  playerId: string;
  currency: string;
  initialBalance?: Money;
  openingTransactionId?: string;
}

export interface RehydrateWalletProps {
  id: string;
  playerId: string;
  balance: Money;
  status: WalletStatus;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export class Wallet {
  protected constructor(
    public readonly id: string,
    public readonly playerId: string,
    private _balance: Money,
    private _status: WalletStatus,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: OpenWalletProps): { wallet: Wallet; openingEntry: WalletLedgerEntry | null } {
    const id = props.id ?? randomUUID();
    const zero = Money.zero(props.currency);
    const initial = props.initialBalance ?? zero;

    if (initial.currency !== zero.currency) {
      throw new DomainError(
        LedgerFailureCode.CURRENCY_MISMATCH,
        `Initial balance currency (${initial.currency}) does not match wallet currency (${zero.currency})`,
      );
    }

    const now = new Date();
    const wallet = new Wallet(id, props.playerId, initial, WalletStatus.ACTIVE, 1, now, now);

    let openingEntry: WalletLedgerEntry | null = null;
    if (initial.isGreaterThanZero()) {
      if (!props.openingTransactionId) {
        throw new DomainError(
          LedgerFailureCode.INVALID_LEDGER_ENTRY_INVARIANT,
          'openingTransactionId is required when opening a wallet with balance > 0.00',
        );
      }
      openingEntry = WalletLedgerEntry.create({
        walletId: wallet.id,
        transactionId: props.openingTransactionId,
        direction: LedgerEntryDirection.CREDIT,
        amount: initial,
        balanceBefore: zero,
        balanceAfter: initial,
        createdAt: now,
      });
    }

    return { wallet, openingEntry };
  }

  static rehydrate(props: RehydrateWalletProps): Wallet {
    return new Wallet(
      props.id,
      props.playerId,
      props.balance,
      props.status,
      props.version,
      props.createdAt,
      props.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get status(): WalletStatus {
    return this._status;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  debit(amount: Money, transactionId: string): WalletLedgerEntry | null {
    this.assertActive();
    const balanceBefore = this._balance;
    const balanceAfter = balanceBefore.subtract(amount);

    if (amount.isZero()) {
      return null;
    }

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = new Date();

    return WalletLedgerEntry.create({
      walletId: this.id,
      transactionId,
      direction: LedgerEntryDirection.DEBIT,
      amount,
      balanceBefore,
      balanceAfter,
      createdAt: this._updatedAt,
    });
  }

  credit(amount: Money, transactionId: string): WalletLedgerEntry | null {
    this.assertActive();
    const balanceBefore = this._balance;
    const balanceAfter = balanceBefore.add(amount);

    if (amount.isZero()) {
      return null;
    }

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = new Date();

    return WalletLedgerEntry.create({
      walletId: this.id,
      transactionId,
      direction: LedgerEntryDirection.CREDIT,
      amount,
      balanceBefore,
      balanceAfter,
      createdAt: this._updatedAt,
    });
  }

  private assertActive(): void {
    if (this._status !== WalletStatus.ACTIVE) {
      throw new DomainError(LedgerFailureCode.WALLET_SUSPENDED, `Wallet ${this.id} is suspended`);
    }
  }
}
