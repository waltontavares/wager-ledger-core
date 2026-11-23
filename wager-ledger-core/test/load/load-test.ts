import { randomUUID } from 'node:crypto';
import { MikroORM } from '@mikro-orm/postgresql';
import mikroOrmConfig from '../../src/mikro-orm.config.js';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/persistence/repositories/mikro-orm.uow.js';
import {
  OpenWalletUseCase,
  ProcessWagerTransactionUseCase,
  ReconcileWalletUseCase,
} from '../../src/application/use-cases/ledger.use-cases.js';
import { WagerTransactionKind } from '../../src/domain/entities/wager-transaction.entity.js';
import { PrometheusMetricsRegistry } from '../../src/infrastructure/observability/observability.service.js';

function percentile(sorted: Array<number>, p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx]!;
}

async function runLoadTest() {
  const TOTAL_WALLETS = 10;
  const TX_PER_WALLET = 30; // 300 transações concorrentes
  const orm = await MikroORM.init(mikroOrmConfig);
  await orm.getMigrator().up();

  const silentLogger = { info: () => {}, warn: () => {}, error: () => {} };
  const metrics = new PrometheusMetricsRegistry();

  const createUow = () => new MikroOrmUnitOfWork(orm.em.fork());
  const walletIds = new Array<string>();

  for (let i = 0; i < TOTAL_WALLETS; i++) {
    const w = await new OpenWalletUseCase(createUow()).execute(
      `load-player-${randomUUID().slice(0, 8)}`,
      'BRL',
      '1000.00',
    );
    walletIds.push(w.id);
  }

  const latencies = new Array<number>();
  let errors = 0;
  const startAll = performance.now();

  const tasks = walletIds.flatMap((walletId, wIdx) =>
    Array.from({ length: TX_PER_WALLET }, async (_, txIdx) => {
      const t0 = performance.now();
      try {
        await new ProcessWagerTransactionUseCase(createUow(), silentLogger, metrics).execute({
          providerId: 'load-provider',
          idempotencyKey: `load-${walletId}-${txIdx}`,
          externalTransactionId: `ext-load-${wIdx}-${txIdx}-${randomUUID().slice(0, 6)}`,
          walletId,
          kind: txIdx % 3 === 0 ? WagerTransactionKind.WIN : WagerTransactionKind.BET,
          amount: '10.00',
          currency: 'BRL',
        });
      } catch {
        errors++;
      } finally {
        latencies.push(performance.now() - t0);
      }
    }),
  );

  await Promise.all(tasks);
  const totalSec = (performance.now() - startAll) / 1000;
  latencies.sort((a, b) => a - b);

  // Reconciliar todas as carteiras após carga
  let allConsistent = true;
  for (const walletId of walletIds) {
    const rep = await new ReconcileWalletUseCase(createUow(), silentLogger, metrics).execute(walletId);
    if (!rep.consistent) allConsistent = false;
  }

  const outboxLagRes = await orm.em
    .fork()
    .getConnection()
    .execute<Array<{ pending: number }>>(
      `SELECT COUNT(*)::int AS pending FROM outbox_messages WHERE published_at IS NULL`,
    );

  console.log(
    JSON.stringify(
      {
        methodology: `${TOTAL_WALLETS} wallets x ${TX_PER_WALLET} concurrent txs (${latencies.length} total)`,
        throughputTxPerSec: Number((latencies.length / totalSec).toFixed(2)),
        latencyMs: {
          p50: Number(percentile(latencies, 50).toFixed(2)),
          p95: Number(percentile(latencies, 95).toFixed(2)),
          p99: Number(percentile(latencies, 99).toFixed(2)),
        },
        errorRatePct: Number(((errors / latencies.length) * 100).toFixed(2)),
        outboxPendingLag: outboxLagRes.at(0)?.pending ?? 0,
        allWalletsReconciled: allConsistent,
      },
      null,
      2,
    ),
  );

  await orm.close(true);
}

await runLoadTest();
