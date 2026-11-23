import { DecimalType } from '@mikro-orm/core';
import { Entity, PrimaryKey, Property, Unique, Index } from '@mikro-orm/decorators/legacy';

@Entity({ tableName: 'wallets' })
@Unique({ properties: ['playerId', 'currency'] })
export class WalletOrmEntity {
  @PrimaryKey({ type: 'uuid' })
  id!: string;

  @Property({ fieldName: 'player_id', type: 'string' })
  playerId!: string;

  @Property({ type: 'string', length: 3 })
  currency!: string;

  @Property({ fieldName: 'balance_amount', type: DecimalType })
  balanceAmount!: string;

  @Property({ type: 'string', length: 20 })
  status!: string;

  @Property({ type: 'integer', version: true })
  version: number = 1;

  @Property({ fieldName: 'created_at', type: 'Date' })
  createdAt: Date = new Date();

  @Property({ fieldName: 'updated_at', type: 'Date' })
  updatedAt: Date = new Date();
}

@Entity({ tableName: 'wager_transactions' })
@Unique({ properties: ['providerId', 'idempotencyKey'] })
@Unique({ properties: ['providerId', 'externalTransactionId'] })
export class WagerTransactionOrmEntity {
  @PrimaryKey({ type: 'uuid' })
  id!: string;

  @Property({ fieldName: 'provider_id', type: 'string' })
  providerId!: string;

  @Property({ fieldName: 'idempotency_key', type: 'string' })
  idempotencyKey!: string;

  @Property({ fieldName: 'external_transaction_id', type: 'string' })
  externalTransactionId!: string;

  @Property({ fieldName: 'wallet_id', type: 'uuid' })
  @Index()
  walletId!: string;

  @Property({ type: 'string', length: 20 })
  kind!: string;

  @Property({ type: DecimalType })
  amount!: string;

  @Property({ type: 'string', length: 3 })
  currency!: string;

  @Property({ type: 'string', length: 30 })
  @Index()
  status!: string;

  @Property({ fieldName: 'payload_hash', type: 'string', length: 64 })
  payloadHash!: string;

  @Property({ fieldName: 'reference_external_transaction_id', type: 'string', nullable: true })
  referenceExternalTransactionId?: string;

  @Property({ fieldName: 'round_id', type: 'string', nullable: true })
  roundId?: string;

  @Property({ fieldName: 'game_id', type: 'string', nullable: true })
  gameId?: string;

  @Property({ fieldName: 'failure_code', type: 'string', nullable: true })
  failureCode?: string;

  @Property({ fieldName: 'failure_reason', type: 'string', nullable: true })
  failureReason?: string;

  @Property({ fieldName: 'retry_attempts', type: 'integer' })
  retryAttempts: number = 0;

  @Property({ fieldName: 'next_retry_at', type: 'Date', nullable: true })
  nextRetryAt?: Date | null;

  @Property({ fieldName: 'created_at', type: 'Date' })
  createdAt: Date = new Date();

  @Property({ fieldName: 'updated_at', type: 'Date' })
  updatedAt: Date = new Date();
}

@Entity({ tableName: 'wallet_ledger_entries' })
@Unique({ properties: ['transactionId', 'walletId'] })
export class WalletLedgerEntryOrmEntity {
  @PrimaryKey({ type: 'uuid' })
  id!: string;

  @Property({ fieldName: 'wallet_id', type: 'uuid' })
  @Index()
  walletId!: string;

  @Property({ fieldName: 'transaction_id', type: 'uuid' })
  transactionId!: string;

  @Property({ type: 'string', length: 10 })
  direction!: string;

  @Property({ type: DecimalType })
  amount!: string;

  @Property({ fieldName: 'balance_before', type: DecimalType })
  balanceBefore!: string;

  @Property({ fieldName: 'balance_after', type: DecimalType })
  balanceAfter!: string;

  @Property({ type: 'string', length: 3 })
  currency!: string;

  @Property({ fieldName: 'created_at', type: 'Date' })
  createdAt: Date = new Date();
}

@Entity({ tableName: 'inbox_messages' })
export class InboxMessageOrmEntity {
  @PrimaryKey({ fieldName: 'consumer_name', type: 'string' })
  consumerName!: string;

  @PrimaryKey({ fieldName: 'message_id', type: 'string' })
  messageId!: string;

  @Property({ fieldName: 'received_at', type: 'Date' })
  receivedAt: Date = new Date();

  @Property({ fieldName: 'processed_at', type: 'Date', nullable: true })
  processedAt: Date | null = null;
}

@Entity({ tableName: 'outbox_messages' })
export class OutboxMessageOrmEntity {
  @PrimaryKey({ type: 'uuid' })
  id!: string;

  @Property({ fieldName: 'event_type', type: 'string' })
  eventType!: string;

  @Property({ fieldName: 'aggregate_id', type: 'uuid' })
  aggregateId!: string;

  @Property({ fieldName: 'correlation_id', type: 'string' })
  correlationId!: string;

  @Property({ fieldName: 'causation_id', type: 'string' })
  causationId!: string;

  @Property({ fieldName: 'occurred_at', type: 'Date' })
  occurredAt!: Date;

  @Property({ type: 'integer' })
  version: number = 1;

  @Property({ type: 'jsonb' })
  envelope!: Record<string, unknown>;

  @Property({ type: 'integer' })
  attempts: number = 0;

  @Property({ fieldName: 'next_attempt_at', type: 'Date' })
  nextAttemptAt: Date = new Date();

  @Property({ fieldName: 'published_at', type: 'Date', nullable: true })
  publishedAt: Date | null = null;

  @Property({ fieldName: 'created_at', type: 'Date' })
  createdAt: Date = new Date();
}
