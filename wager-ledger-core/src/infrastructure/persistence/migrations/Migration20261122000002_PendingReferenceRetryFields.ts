import { Migration } from '@mikro-orm/migrations';

export class Migration20261122000002_PendingReferenceRetryFields extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      ALTER TABLE wager_transactions
      ADD COLUMN retry_attempts INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN next_retry_at TIMESTAMPTZ NULL;
    `);

    this.addSql(`
      DROP INDEX IF EXISTS idx_wager_tx_pending_ref_status;
    `);

    this.addSql(`
      CREATE INDEX idx_wager_tx_pending_ref_due
      ON wager_transactions (next_retry_at, created_at)
      WHERE status = 'PENDING_REFERENCE';
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`DROP INDEX IF EXISTS idx_wager_tx_pending_ref_due;`);
    this.addSql(`
      ALTER TABLE wager_transactions
      DROP COLUMN IF EXISTS next_retry_at,
      DROP COLUMN IF EXISTS retry_attempts;
    `);
  }
}
