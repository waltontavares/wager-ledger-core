import { Migration } from '@mikro-orm/migrations';

export class Migration20261122000001_InitialLedgerSchema extends Migration {
  override async up(): Promise<void> {
    // 1. WALLETS
    this.addSql(`
      CREATE TABLE wallets (
        id UUID PRIMARY KEY,
        player_id VARCHAR(128) NOT NULL,
        currency CHAR(3) NOT NULL,
        balance_amount NUMERIC(20, 2) NOT NULL DEFAULT 0.00,
        status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
        version INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency),
        CONSTRAINT chk_wallets_balance_non_negative CHECK (balance_amount >= 0),
        CONSTRAINT chk_wallets_version_positive CHECK (version >= 1)
      );
    `);

    // 2. WAGER TRANSACTIONS
    this.addSql(`
      CREATE TABLE wager_transactions (
        id UUID PRIMARY KEY,
        provider_id VARCHAR(128) NOT NULL,
        idempotency_key VARCHAR(255) NOT NULL,
        external_transaction_id VARCHAR(255) NOT NULL,
        wallet_id UUID NOT NULL REFERENCES wallets(id),
        kind VARCHAR(20) NOT NULL,
        amount NUMERIC(20, 2) NOT NULL,
        currency CHAR(3) NOT NULL,
        status VARCHAR(30) NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        reference_external_transaction_id VARCHAR(255) NULL,
        round_id VARCHAR(255) NULL,
        game_id VARCHAR(255) NULL,
        failure_code VARCHAR(64) NULL,
        failure_reason TEXT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_wager_tx_provider_idempotency UNIQUE (provider_id, idempotency_key),
        CONSTRAINT uq_wager_tx_provider_external_id UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT chk_wager_tx_amount_non_negative CHECK (amount >= 0)
      );
    `);

    this.addSql(`
      CREATE INDEX idx_wager_tx_provider_round
      ON wager_transactions (provider_id, round_id)
      WHERE round_id IS NOT NULL;
    `);

    this.addSql(`
      CREATE INDEX idx_wager_tx_provider_reference
      ON wager_transactions (provider_id, reference_external_transaction_id)
      WHERE reference_external_transaction_id IS NOT NULL;
    `);

    this.addSql(`
      CREATE INDEX idx_wager_tx_pending_ref_status
      ON wager_transactions (status, created_at)
      WHERE status = 'PENDING_REFERENCE';
    `);

    // 3. WALLET LEDGER ENTRIES (Append-only / Imutável)
    this.addSql(`
      CREATE TABLE wallet_ledger_entries (
        id UUID PRIMARY KEY,
        wallet_id UUID NOT NULL REFERENCES wallets(id),
        transaction_id UUID NOT NULL REFERENCES wager_transactions(id),
        direction VARCHAR(10) NOT NULL,
        amount NUMERIC(20, 2) NOT NULL,
        balance_before NUMERIC(20, 2) NOT NULL,
        balance_after NUMERIC(20, 2) NOT NULL,
        currency CHAR(3) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_ledger_entry_tx_wallet UNIQUE (transaction_id, wallet_id),
        CONSTRAINT chk_ledger_entry_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
        CONSTRAINT chk_ledger_entry_amount_positive CHECK (amount > 0),
        CONSTRAINT chk_ledger_entry_balance_after_non_negative CHECK (balance_after >= 0)
      );
    `);

    this.addSql(`
      CREATE INDEX idx_ledger_entries_wallet_created
      ON wallet_ledger_entries (wallet_id, created_at);
    `);

    // Trigger física para impedir UPDATE e DELETE em wallet_ledger_entries
    this.addSql(`
      CREATE OR REPLACE FUNCTION prevent_ledger_entry_mutation()
      RETURNS TRIGGER AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is immutable: % operation is forbidden', TG_OP;
      END;
      $$ LANGUAGE plpgsql;
    `);

    this.addSql(`
      CREATE TRIGGER trg_prevent_ledger_entry_mutation
      BEFORE UPDATE OR DELETE ON wallet_ledger_entries
      FOR EACH ROW
      EXECUTE FUNCTION prevent_ledger_entry_mutation();
    `);

    // 4. INBOX MESSAGES (Deduplicação de consumo)
    this.addSql(`
      CREATE TABLE inbox_messages (
        consumer_name VARCHAR(128) NOT NULL,
        message_id VARCHAR(255) NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ NULL,
        PRIMARY KEY (consumer_name, message_id)
      );
    `);

    // 5. OUTBOX MESSAGES (Transactional Outbox com índice parcial)
    this.addSql(`
      CREATE TABLE outbox_messages (
        id UUID PRIMARY KEY,
        event_type VARCHAR(128) NOT NULL,
        aggregate_id UUID NOT NULL,
        correlation_id VARCHAR(255) NOT NULL,
        causation_id VARCHAR(255) NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        envelope JSONB NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        published_at TIMESTAMPTZ NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    this.addSql(`
      CREATE INDEX idx_outbox_pending_dispatch
      ON outbox_messages (next_attempt_at, occurred_at)
      WHERE published_at IS NULL;
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`DROP TABLE IF EXISTS outbox_messages CASCADE;`);
    this.addSql(`DROP TABLE IF EXISTS inbox_messages CASCADE;`);
    this.addSql(`DROP TRIGGER IF EXISTS trg_prevent_ledger_entry_mutation ON wallet_ledger_entries;`);
    this.addSql(`DROP FUNCTION IF EXISTS prevent_ledger_entry_mutation();`);
    this.addSql(`DROP TABLE IF EXISTS wallet_ledger_entries CASCADE;`);
    this.addSql(`DROP TABLE IF EXISTS wager_transactions CASCADE;`);
    this.addSql(`DROP TABLE IF EXISTS wallets CASCADE;`);
  }
}
