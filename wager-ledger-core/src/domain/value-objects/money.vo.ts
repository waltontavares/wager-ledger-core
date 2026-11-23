import { Decimal } from 'decimal.js';
import { DomainError, LedgerFailureCode } from '../errors/ledger.errors.js';

export interface MoneyProps {
  amount: string;
  currency: string;
}

const STRICT_TWO_DECIMALS_REGEX = /^\d+\.\d{2}$/;
const ISO_CURRENCY_REGEX = /^[A-Z]{3}$/;

export class Money {
  private readonly value: Decimal;
  public readonly currency: string;

  private constructor(value: Decimal, currency: string) {
    if (!value.isFinite() || value.isNaN()) {
      throw new DomainError(
        LedgerFailureCode.INVALID_MONEY_FORMAT,
        'Money value must be a finite decimal',
      );
    }
    if (value.isNegative()) {
      throw new DomainError(
        LedgerFailureCode.NEGATIVE_AMOUNT_NOT_ALLOWED,
        `Negative money amount is not allowed: ${value.toFixed(2)}`,
      );
    }
    if (!ISO_CURRENCY_REGEX.test(currency)) {
      throw new DomainError(
        LedgerFailureCode.CURRENCY_MISMATCH,
        `Invalid ISO-4217 currency code: ${currency}`,
      );
    }

    this.value = value.toDecimalPlaces(2, Decimal.ROUND_HALF_EVEN);
    this.currency = currency;
    Object.freeze(this);
  }

  static fromContract(amount: string, currency: string): Money {
    if (typeof amount !== 'string' || !STRICT_TWO_DECIMALS_REGEX.test(amount)) {
      throw new DomainError(
        LedgerFailureCode.INVALID_MONEY_FORMAT,
        `Contract amount must be a non-negative decimal string with 2 decimal places (received: '${String(amount)}')`,
      );
    }
    const normalizedCurrency = currency.trim().toUpperCase();
    return new Money(new Decimal(amount), normalizedCurrency);
  }

  static zero(currency: string): Money {
    return new Money(new Decimal('0.00'), currency.trim().toUpperCase());
  }

  get amount(): string {
    return this.value.toFixed(2);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isGreaterThanZero(): boolean {
    return this.value.greaterThan(0);
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.value.equals(other.value);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    if (this.value.lessThan(other.value)) {
      throw new DomainError(
        LedgerFailureCode.INSUFFICIENT_FUNDS,
        `Insufficient funds: required ${other.amount} ${other.currency}, available ${this.amount} ${this.currency}`,
      );
    }
    return new Money(this.value.minus(other.value), this.currency);
  }

  toProps(): MoneyProps {
    return {
      amount: this.amount,
      currency: this.currency,
    };
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new DomainError(
        LedgerFailureCode.CURRENCY_MISMATCH,
        `Currency mismatch: ${this.currency} vs ${other.currency}`,
      );
    }
  }
}
