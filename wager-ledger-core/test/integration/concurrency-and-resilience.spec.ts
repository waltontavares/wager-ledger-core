import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { MikroORM } from '@mikro-orm/postgresql';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import mikroOrmConfig from '../../src/mikro-orm.config.js';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/persistence/repositories/mikro-orm.uow.js';
import {
  OpenWalletUseCase,
  ProcessWagerTransactionUseCase,
  ReconcileWalletUseCase,
  ReplayPendingReferencesUseCase,
} from '../../src/application/use-cases/ledger.use-cases.js';
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/entities/wager-transaction.entity.js';
import { LedgerFailureCode } from '../../src/domain/errors/ledger.errors.js';
import {
  PrometheusMetricsRegistry,
  StructuredJsonLogger,
} from '../../src/infrastructure/observability/observability.service.js';
import { LedgerBackgroundWorkers } from '../../src/infrastructure/messaging/workers.service.js';

const SQS_ENDPOINT = process.env.AWS_ENDPOINT ?? 'http://localhost:4566';
const AWS_REGION = process.env.AWS_REGION ?? 'us-east-1';

describe('Integração Real & Concorrência (PostgreSQL 17 + MiniStack SQS)', () => {
  let orm: MikroORM;
  let sqs: SQSClient;
  let testQueueUrl: string;
  let testDlqUrl: string;

  const logger = new StructuredJsonLogger();
  const metrics = new PrometheusMetricsRegistry();

  function createIsolatedAppServices(queueUrl = testQueueUrl, dlqUrl = testDlqUrl) {
    const em = orm.em.fork();
    const uow = new MikroOrmUnitOfWork(em);
    const openWallet = new OpenWalletUseCase(uow);
    const processWager = new ProcessWagerTransactionUseCase(uow, logger, metrics, {
      maxAttempts: 5,
      ttlMs: 300_000,
      baseDelayMs: 50, // Rápido para testes de integração
      maxDelayMs: 200,
    });
    const reconcileWallet = new ReconcileWalletUseCase(uow, logger, metrics);
    const replayPending = new ReplayPendingReferencesUseCase(uow, processWager, logger);

    process.env.SQS_QUEUE_URL = queueUrl;
    process.env.SQS_DLQ_URL = dlqUrl;
    process.env.SQS_OUTBOX_QUEUE_URL = queueUrl;

    const worker = new LedgerBackgroundWorkers(uow, processWager, replayPending, logger, metrics);
    return { em, uow, openWallet, processWager, reconcileWallet, replayPending, worker };
  }

  async function assertLedgerConsistent(walletId: string, expectedBalance?: string) {
    const { reconcileWallet, uow } = createIsolatedAppServices();
    const report = await reconcileWallet.execute(walletId);
    expect(report.consistent).toBe(true);
    expect(report.difference).toBe('0.00');

    if (expectedBalance !== undefined) {
      expect(report.walletBalance).toBe(expectedBalance);
      const wallet = await uow.runInTransaction((ctx) => ctx.wallets.findById(walletId));
      expect(wallet?.balance.amount).toBe(expectedBalance);
    }
    return report;
  }

  beforeAll(async () => {
    orm = await MikroORM.init(mikroOrmConfig);
    await orm.getMigrator().up();

    sqs = new SQSClient({
      region: AWS_REGION,
      endpoint: SQS_ENDPOINT,
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    });

    const suffix = randomUUID().slice(0, 8);
    const dlqRes = await sqs.send(
      new CreateQueueCommand({
        QueueName: `test-wager-dlq-${suffix}.fifo`,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'true' },
      }),
    );
    testDlqUrl = dlqRes.QueueUrl!;

    const qRes = await sqs.send(
      new CreateQueueCommand({
        QueueName: `test-wager-main-${suffix}.fifo`,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'true' },
      }),
    );
    testQueueUrl = qRes.QueueUrl!;
  });

  beforeEach(async () => {
    const conn = orm.em.fork().getConnection();
    await conn.execute('DELETE FROM outbox_messages;');
    await conn.execute('DELETE FROM inbox_messages;');
    // wallet_ledger_entries tem trigger de imutabilidade no banco; limpa em ambiente de teste desabilitando a trigger temporariamente
    await conn.execute('ALTER TABLE wallet_ledger_entries DISABLE TRIGGER trg_prevent_ledger_entry_mutation;');
    await conn.execute('DELETE FROM wallet_ledger_entries;');
    await conn.execute('ALTER TABLE wallet_ledger_entries ENABLE TRIGGER trg_prevent_ledger_entry_mutation;');
    await conn.execute('DELETE FROM wager_transactions;');
    await conn.execute('DELETE FROM wallets;');

    try {
      await sqs.send(new PurgeQueueCommand({ QueueUrl: testQueueUrl }));
    } catch {
      // Ignora se a fila já estiver vazia
    }
  });

  afterAll(async () => {
    sqs.destroy();
    await orm.close(true);
  });

  // ==================== CENÁRIO 1 ====================
  test('Cenário 1: Mesma aposta disparada 50x em paralelo -> exatamente 1 débito e 49 replays', async () => {
    const { openWallet } = createIsolatedAppServices();
    const wallet = await openWallet.execute('player-c1', 'BRL', '500.00');

    const requests = Array.from({ length: 50 }, () => {
      const { processWager } = createIsolatedAppServices();
      return processWager.execute({
        providerId: 'pragmatic',
        idempotencyKey: 'idem-same-50x',
        externalTransactionId: 'ext-same-50x',
        walletId: wallet.id,
        kind: WagerTransactionKind.BET,
        amount: '10.00',
        currency: 'BRL',
      });
    });

    const results = await Promise.all(requests);
    const nonReplays = results.filter((r) => !r.idempotentReplay);
    const replays = results.filter((r) => r.idempotentReplay);

    expect(nonReplays.length).toBe(1);
    expect(replays.length).toBe(49);
    for (const r of results) {
      expect(r.observedBalance.amount).toBe('490.00');
    }

    const report = await assertLedgerConsistent(wallet.id, '490.00');
    // 1 OPENING + 1 BET = 2 lançamentos
    expect(report.checkedEntries).toBe(2);
  });

  // ==================== CENÁRIO 2 ====================
  test('Cenário 2: Saldo 100.00 + duas apostas de 80.00 simultâneas -> 1 PROCESSED, 1 REJECTED, saldo 20.00, 1 DEBIT', async () => {
    const { openWallet } = createIsolatedAppServices();
    const wallet = await openWallet.execute('player-c2', 'BRL', '100.00');

    const [resA, resB] = await Promise.all([
      createIsolatedAppServices().processWager.execute({
        providerId: 'evolution',
        idempotencyKey: 'idem-race-a',
        externalTransactionId: 'ext-race-a',
        walletId: wallet.id,
        kind: WagerTransactionKind.BET,
        amount: '80.00',
        currency: 'BRL',
      }),
      createIsolatedAppServices().processWager.execute({
        providerId: 'evolution',
        idempotencyKey: 'idem-race-b',
        externalTransactionId: 'ext-race-b',
        walletId: wallet.id,
        kind: WagerTransactionKind.BET,
        amount: '80.00',
        currency: 'BRL',
      }),
    ]);

    const statuses = [resA.transaction.status, resB.transaction.status].sort();
    expect(statuses).toEqual([WagerTransactionStatus.PROCESSED, WagerTransactionStatus.REJECTED]);

    const rejected = resA.transaction.status === WagerTransactionStatus.REJECTED ? resA : resB;
    expect(rejected.transaction.failureCode).toBe(LedgerFailureCode.INSUFFICIENT_FUNDS);

    const report = await assertLedgerConsistent(wallet.id, '20.00');
    // 1 OPENING + 1 BET efetivado = 2 lançamentos
    expect(report.checkedEntries).toBe(2);
  });

  // ==================== CENÁRIO 3 ====================
  test('Cenário 3: Wallets distintas em paralelo processam sem bloqueio cruzado e mantêm consistência', async () => {
    const walletIds = new Array<string>();
    for (let i = 0; i < 10; i++) {
      const w = await createIsolatedAppServices().openWallet.execute(`player-c3-${i}`, 'USD', '100.00');
      walletIds.push(w.id);
    }

    // 3 apostas de 15.00 para cada uma das 10 carteiras em paralelo (30 transações simultâneas)
    const tasks = walletIds.flatMap((walletId, wIdx) =>
      [1, 2, 3].map((betIdx) =>
        createIsolatedAppServices().processWager.execute({
          providerId: 'netent',
          idempotencyKey: `idem-w${wIdx}-b${betIdx}`,
          externalTransactionId: `ext-w${wIdx}-b${betIdx}`,
          walletId,
          kind: WagerTransactionKind.BET,
          amount: '15.00',
          currency: 'USD',
        }),
      ),
    );

    const results = await Promise.all(tasks);
    expect(results.every((r) => r.transaction.status === WagerTransactionStatus.PROCESSED)).toBe(true);

    for (const walletId of walletIds) {
      await assertLedgerConsistent(walletId, '55.00');
    }
  });

  // ==================== CENÁRIO 4 ====================
  test('Cenário 4: >= 3 workers SQS simultâneos consumindo mensagens de múltiplas wallets sem duplicidade', async () => {
    const w1 = await createIsolatedAppServices().openWallet.execute('player-c4-1', 'BRL', '200.00');
    const w2 = await createIsolatedAppServices().openWallet.execute('player-c4-2', 'BRL', '200.00');
    const w3 = await createIsolatedAppServices().openWallet.execute('player-c4-3', 'BRL', '200.00');
    const targets = [w1.id, w2.id, w3.id];

    // Publica 15 mensagens (5 por wallet) no SQS FIFO real
    for (let i = 0; i < 15; i++) {
      const walletId = targets[i % 3]!;
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: testQueueUrl,
          MessageGroupId: walletId,
          MessageDeduplicationId: `msg-c4-${i}`,
          MessageBody: JSON.stringify({
            providerId: 'pragmatic',
            idempotencyKey: `idem-sqs-c4-${i}`,
            externalTransactionId: `ext-sqs-c4-${i}`,
            walletId,
            kind: WagerTransactionKind.BET,
            amount: '10.00',
            currency: 'BRL',
          }),
        }),
      );
    }

    // Inicia 3 instâncias simultâneas de workers
    const wInst1 = createIsolatedAppServices().worker;
    const wInst2 = createIsolatedAppServices().worker;
    const wInst3 = createIsolatedAppServices().worker;

    wInst1.onModuleInit();
    wInst2.onModuleInit();
    wInst3.onModuleInit();

    // Aguarda até que todas as 15 apostas sejam processadas
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      const countRows = await orm.em
        .fork()
        .getConnection()
        .execute<Array<{ cnt: number }>>(
          `SELECT COUNT(*)::int AS cnt FROM wager_transactions WHERE kind = 'BET' AND status = 'PROCESSED'`,
        );
      if ((countRows.at(0)?.cnt ?? 0) === 15) break;
      await new Promise((r) => setTimeout(r, 150));
    }

    await Promise.all([
      wInst1.beforeApplicationShutdown('TEST'),
      wInst2.beforeApplicationShutdown('TEST'),
      wInst3.beforeApplicationShutdown('TEST'),
    ]);

    for (const walletId of targets) {
      await assertLedgerConsistent(walletId, '150.00');
    }
  });

  // ==================== CENÁRIO 5 ====================
  test('Cenário 5: Worker morto após COMMIT e antes do ack (DeleteMessage) -> redelivery sem duplicar efeito', async () => {
    const wallet = await createIsolatedAppServices().openWallet.execute('player-c5', 'BRL', '100.00');

    await sqs.send(
      new SendMessageCommand({
        QueueUrl: testQueueUrl,
        MessageGroupId: wallet.id,
        MessageDeduplicationId: 'msg-crash-before-ack',
        MessageBody: JSON.stringify({
          providerId: 'pragmatic',
          idempotencyKey: 'idem-crash-1',
          externalTransactionId: 'ext-crash-1',
          walletId: wallet.id,
          kind: WagerTransactionKind.BET,
          amount: '35.00',
          currency: 'BRL',
        }),
      }),
    );

    // 1. Primeira leitura: processa e commita no banco, mas simula crash ANTES de chamar DeleteMessage
    const recv1 = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: testQueueUrl,
        MaxNumberOfMessages: 1,
        VisibilityTimeout: 1, // Expira em 1s simulando queda do worker sem ack
      }),
    );
    const msg1 = recv1.Messages?.at(0);
    expect(msg1).toBeDefined();

    const parsed1 = JSON.parse(msg1!.Body!);
    const firstRun = await createIsolatedAppServices().processWager.execute({
      providerId: parsed1.providerId,
      idempotencyKey: parsed1.idempotencyKey,
      externalTransactionId: parsed1.externalTransactionId,
      walletId: parsed1.walletId,
      kind: parsed1.kind,
      amount: parsed1.amount,
      currency: parsed1.currency,
      inbox: { consumerName: 'wager-sqs-consumer', messageId: msg1!.MessageId! },
    });
    expect(firstRun.transaction.status).toBe(WagerTransactionStatus.PROCESSED);
    // NÃO chama DeleteMessageCommand (simula morte do worker após COMMIT)

    // Aguarda VisibilityTimeout expirar e SQS reentregar a mesma mensagem
    await new Promise((r) => setTimeout(r, 1500));

    const recv2 = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: testQueueUrl,
        MaxNumberOfMessages: 1,
      }),
    );
    const msg2 = recv2.Messages?.at(0);
    expect(msg2).toBeDefined();
    expect(msg2!.MessageId).toBe(msg1!.MessageId);

    // 2. Segunda instância processa a redelivery -> bloqueada pela InboxMessage sem duplicar débito
    const secondRun = await createIsolatedAppServices().processWager.execute({
      providerId: parsed1.providerId,
      idempotencyKey: parsed1.idempotencyKey,
      externalTransactionId: parsed1.externalTransactionId,
      walletId: parsed1.walletId,
      kind: parsed1.kind,
      amount: parsed1.amount,
      currency: parsed1.currency,
      inbox: { consumerName: 'wager-sqs-consumer', messageId: msg2!.MessageId! },
    });
    expect(secondRun.inboxDuplicate).toBe(true);
    await sqs.send(
      new DeleteMessageCommand({ QueueUrl: testQueueUrl, ReceiptHandle: msg2!.ReceiptHandle! }),
    );

    await assertLedgerConsistent(wallet.id, '65.00');
  });

  // ==================== CENÁRIO 6 ====================
  test('Cenário 6: Dois publishers concorrentes na mesma Outbox (FOR UPDATE SKIP LOCKED) sem duplicidade', async () => {
    const wallet = await createIsolatedAppServices().openWallet.execute('player-c6', 'BRL', '500.00');

    // Gera 10 transações (20 eventos na Outbox: WagerTransactionProcessed + WalletBalanceChanged)
    for (let i = 0; i < 10; i++) {
      await createIsolatedAppServices().processWager.execute({
        providerId: 'pragmatic',
        idempotencyKey: `idem-outbox-${i}`,
        externalTransactionId: `ext-outbox-${i}`,
        walletId: wallet.id,
        kind: WagerTransactionKind.BET,
        amount: '5.00',
        currency: 'BRL',
      });
    }

    const pub1 = createIsolatedAppServices().worker;
    const pub2 = createIsolatedAppServices().worker;

    // Dispara flushOutbox simultaneamente em 2 instâncias
    const [count1, count2] = await Promise.all([pub1.flushOutbox(20), pub2.flushOutbox(20)]);

    expect(count1 + count2).toBe(20);

    const pendingRows = await orm.em
      .fork()
      .getConnection()
      .execute<Array<{ cnt: number }>>(
        `SELECT COUNT(*)::int AS cnt FROM outbox_messages WHERE published_at IS NULL`,
      );
    expect(pendingRows.at(0)?.cnt).toBe(0);

    await assertLedgerConsistent(wallet.id, '450.00');
  });

  // ==================== CENÁRIO 7 ====================
  test('Cenário 7: REFUND/ROLLBACK chegando antes da referência -> PENDING_REFERENCE e resolução posterior via Reaper', async () => {
    const wallet = await createIsolatedAppServices().openWallet.execute('player-c7', 'BRL', '100.00');

    // 1. REFUND chega ANTES do BET existir
    const refundPending = await createIsolatedAppServices().processWager.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-ooo-refund',
      externalTransactionId: 'ext-ooo-refund',
      walletId: wallet.id,
      kind: WagerTransactionKind.REFUND,
      amount: '40.00',
      currency: 'BRL',
      referenceExternalTransactionId: 'ext-ooo-bet',
      roundId: 'round-ooo',
    });
    expect(refundPending.transaction.status).toBe(WagerTransactionStatus.PENDING_REFERENCE);
    await assertLedgerConsistent(wallet.id, '100.00');

    // 2. BET original chega depois -> saldo vai a 60.00
    const betRes = await createIsolatedAppServices().processWager.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-ooo-bet',
      externalTransactionId: 'ext-ooo-bet',
      walletId: wallet.id,
      kind: WagerTransactionKind.BET,
      amount: '40.00',
      currency: 'BRL',
      roundId: 'round-ooo',
    });
    expect(betRes.transaction.status).toBe(WagerTransactionStatus.PROCESSED);
    await assertLedgerConsistent(wallet.id, '60.00');

    // 3. Aguarda backoff curto (200ms) e roda o Reaper -> REFUND resolve e devolve os 40.00
    await new Promise((r) => setTimeout(r, 250));
    const replayed = await createIsolatedAppServices().replayPending.execute(50);
    expect(replayed).toBe(1);

    await assertLedgerConsistent(wallet.id, '100.00');
  });

  // ==================== CENÁRIO 8 ====================
  test('Cenário 8: Reinício abrupto e substituição de worker comprovando consistência final', async () => {
    const wallet = await createIsolatedAppServices().openWallet.execute('player-c8', 'BRL', '300.00');

    for (let i = 0; i < 8; i++) {
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: testQueueUrl,
          MessageGroupId: wallet.id,
          MessageDeduplicationId: `msg-restart-${i}`,
          MessageBody: JSON.stringify({
            providerId: 'pragmatic',
            idempotencyKey: `idem-restart-${i}`,
            externalTransactionId: `ext-restart-${i}`,
            walletId: wallet.id,
            kind: WagerTransactionKind.BET,
            amount: '10.00',
            currency: 'BRL',
          }),
        }),
      );
    }

    // Worker 1 inicia, processa parcialmente e sofre shutdown
    const worker1 = createIsolatedAppServices().worker;
    worker1.onModuleInit();
    await new Promise((r) => setTimeout(r, 300));
    await worker1.beforeApplicationShutdown('SIGTERM');

    // Worker 2 assume após reinício e drena o restante
    const worker2 = createIsolatedAppServices().worker;
    worker2.onModuleInit();

    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      const rows = await orm.em
        .fork()
        .getConnection()
        .execute<Array<{ cnt: number }>>(
          `SELECT COUNT(*)::int AS cnt FROM wager_transactions WHERE wallet_id = ? AND kind = 'BET' AND status = 'PROCESSED'`,
          [wallet.id],
        );
      if ((rows.at(0)?.cnt ?? 0) === 8) break;
      await new Promise((r) => setTimeout(r, 150));
    }

    await worker2.beforeApplicationShutdown('SIGTERM');

    await assertLedgerConsistent(wallet.id, '220.00');
  });
});
