import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/value-objects/money.vo.js';
import { Wallet, WalletStatus } from '../../src/domain/entities/wallet.entity.js';
import {
  LedgerEntryDirection,
  WalletLedgerEntry,
} from '../../src/domain/entities/wallet-ledger-entry.entity.js';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/entities/wager-transaction.entity.js';
import { InboxMessage, OutboxMessage } from '../../src/domain/entities/messaging.entities.js';
import {
  DomainError,
  InvalidTransactionStateError,
  LedgerFailureCode,
} from '../../src/domain/errors/ledger.errors.js';
import {
  LoggerPort,
  MetricsPort,
  PendingReferenceRecord,
  TransactionContext,
  UnitOfWorkPort,
} from '../../src/application/ports/application.ports.js';
import { ProcessWagerTransactionUseCase } from '../../src/application/use-cases/ledger.use-cases.js';

// ==================== IN-MEMORY UOW PARA TESTES UNITÁRIOS ====================
class InMemoryUnitOfWork implements UnitOfWorkPort {
  public wallets = new Map<string, Wallet>();
  public transactions = new Map<string, { tx: WagerTransaction; idempotencyKey: string }>();
  public ledgerEntries = new Array<WalletLedgerEntry>();
  public outboxMessages = new Array<OutboxMessage>();
  public inboxMessages = new Set<string>();

  async runInTransaction<T>(work: (ctx: TransactionContext) => Promise<T>): Promise<T> {
    const ctx: TransactionContext = {
      wallets: {
        findByIdForUpdate: async (id) => this.wallets.get(id) ?? null,
        findById: async (id) => this.wallets.get(id) ?? null,
        findByPlayerAndCurrency: async (playerId, currency) => {
          for (const w of this.wallets.values()) {
            if (w.playerId === playerId && w.balance.currency === currency.toUpperCase()) {
              return w;
            }
          }
          return null;
        },
        insert: async (wallet) => {
          this.wallets.set(wallet.id, wallet);
        },
        updateWithVersionCheck: async (wallet) => {
          this.wallets.set(wallet.id, wallet);
        },
      },
      transactions: {
        findById: async (id) => this.transactions.get(id)?.tx ?? null,
        findByIdempotencyKey: async (providerId, idempotencyKey) => {
          for (const rec of this.transactions.values()) {
            if (rec.tx.providerId === providerId && rec.idempotencyKey === idempotencyKey) {
              return rec.tx;
            }
          }
          return null;
        },
        findByExternalId: async (providerId, externalTransactionId) => {
          for (const rec of this.transactions.values()) {
            if (
              rec.tx.providerId === providerId &&
              rec.tx.externalTransactionId === externalTransactionId
            ) {
              return rec.tx;
            }
          }
          return null;
        },
        findProcessedReversalByReference: async (providerId, refExtId, kinds) => {
          for (const rec of this.transactions.values()) {
            if (
              rec.tx.providerId === providerId &&
              rec.tx.referenceExternalTransactionId === refExtId &&
              kinds.includes(rec.tx.kind) &&
              rec.tx.status === WagerTransactionStatus.PROCESSED
            ) {
              return rec.tx;
            }
          }
          return null;
        },
        findDuePendingReferences: async () => new Array<PendingReferenceRecord>(),
        save: async (tx, idempotencyKey) => {
          this.transactions.set(tx.id, { tx, idempotencyKey });
        },
      },
      ledgerEntries: {
        save: async (entry) => {
          this.ledgerEntries.push(entry);
        },
        findByTransactionId: async (txId) =>
          this.ledgerEntries.find((e) => e.transactionId === txId) ?? null,
        findPaginatedByWalletId: async () => ({ items: new Array<WalletLedgerEntry>(), nextCursor: null }),
        reconcileSummaryByWalletId: async () => ({ sumAmount: '0.00', checkedEntries: 0 }),
      },
      inbox: {
        tryAcquire: async (msg: InboxMessage) => {
          const key = `${msg.consumerName}:${msg.messageId}`;
          if (this.inboxMessages.has(key)) return false;
          this.inboxMessages.add(key);
          return true;
        },
        markProcessed: async () => {},
      },
      outbox: {
        save: async (msg) => {
          this.outboxMessages.push(msg);
        },
        claimPendingBatchForUpdate: async () => new Array<OutboxMessage>(),
        markPublished: async () => {},
        recordFailure: async () => {},
      },
    };

    return work(ctx);
  }
}

const silentLogger: LoggerPort = {
  info: () => {},
  warn: () => {},
  error: () => {},
};

const noopMetrics: MetricsPort = {
  incrementCounter: () => {},
  observeHistogram: () => {},
};

// ==================== 1. TESTES DE MONEY ====================
describe('Money Value Object', () => {
  test('aceita string decimal exata com 2 casas e realiza operações sem erro de ponto flutuante', () => {
    const a = Money.fromContract('0.10', 'brl');
    const b = Money.fromContract('0.20', 'BRL');
    const sum = a.add(b);

    expect(sum.amount).toBe('0.30');
    expect(sum.currency).toBe('BRL');
    expect(Object.isFrozen(sum)).toBe(true);
  });

  test('rejeita entradas inválidas (negativos, escala diferente de 2, NaN, Infinity, notação científica, number)', () => {
    const invalidInputs: Array<unknown> = [
      '-10.00',
      '10',
      '10.5',
      '10.555',
      '1e2',
      'NaN',
      'Infinity',
      '-Infinity',
      '',
      'abc',
      10.5,
      null,
      undefined,
    ];

    for (const input of invalidInputs) {
      expect(() => Money.fromContract(input as string, 'BRL')).toThrow(DomainError);
    }
  });

  test('bloqueia operações entre moedas diferentes', () => {
    const brl = Money.fromContract('50.00', 'BRL');
    const usd = Money.fromContract('50.00', 'USD');

    expect(() => brl.add(usd)).toThrow(DomainError);
    expect(() => brl.subtract(usd)).toThrow(DomainError);
  });

  test('bloqueia subtração que resultaria em saldo negativo', () => {
    const balance = Money.fromContract('30.00', 'BRL');
    const debit = Money.fromContract('30.01', 'BRL');

    try {
      balance.subtract(debit);
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(DomainError);
      expect((err as DomainError).code).toBe(LedgerFailureCode.INSUFFICIENT_FUNDS);
    }
  });
});

// ==================== 2. TESTES DE WALLET & LEDGER ENTRY ====================
describe('Wallet Aggregate & WalletLedgerEntry', () => {
  test('Wallet.open com saldo > 0.00 inicia em version 1 e gera openingEntry consistente', () => {
    const { wallet, openingEntry } = Wallet.open({
      playerId: 'player-1',
      currency: 'BRL',
      initialBalance: Money.fromContract('150.00', 'BRL'),
      openingTransactionId: 'tx-open-1',
    });

    expect(wallet.version).toBe(1);
    expect(wallet.balance.amount).toBe('150.00');
    expect(openingEntry).not.toBeNull();
    expect(openingEntry?.direction).toBe(LedgerEntryDirection.CREDIT);
    expect(openingEntry?.balanceBefore.amount).toBe('0.00');
    expect(openingEntry?.balanceAfter.amount).toBe('150.00');
  });

  test('debit e credit incrementam version apenas quando o saldo muda e produzem LedgerEntry válido', () => {
    const { wallet } = Wallet.open({ playerId: 'player-2', currency: 'USD' });
    expect(wallet.version).toBe(1);

    // Operação com valor 0.00 não altera saldo nem incrementa version
    const zeroEntry = wallet.credit(Money.zero('USD'), 'tx-zero');
    expect(zeroEntry).toBeNull();
    expect(wallet.version).toBe(1);

    const creditEntry = wallet.credit(Money.fromContract('100.00', 'USD'), 'tx-cred-1');
    expect(wallet.version).toBe(2);
    expect(wallet.balance.amount).toBe('100.00');
    expect(creditEntry?.balanceBefore.amount).toBe('0.00');
    expect(creditEntry?.balanceAfter.amount).toBe('100.00');

    const debitEntry = wallet.debit(Money.fromContract('35.50', 'USD'), 'tx-deb-1');
    expect(wallet.version).toBe(3);
    expect(wallet.balance.amount).toBe('64.50');
    expect(debitEntry?.balanceBefore.amount).toBe('100.00');
    expect(debitEntry?.balanceAfter.amount).toBe('64.50');
  });

  test('WalletLedgerEntry.create valida invariante matemática balanceBefore ± amount === balanceAfter', () => {
    expect(() =>
      WalletLedgerEntry.create({
        walletId: 'w-1',
        transactionId: 'tx-1',
        direction: LedgerEntryDirection.DEBIT,
        amount: Money.fromContract('20.00', 'BRL'),
        balanceBefore: Money.fromContract('100.00', 'BRL'),
        balanceAfter: Money.fromContract('70.00', 'BRL'), // incorreto (deveria ser 80.00)
      }),
    ).toThrow(DomainError);
  });
});

// ==================== 3. TESTES DE WAGER TRANSACTION ====================
describe('WagerTransaction Entity & State Machine', () => {
  test('bloqueia OPENING quando não autorizado explicitamente e exige referência em REFUND/ROLLBACK', () => {
    expect(() =>
      WagerTransaction.create({
        providerId: 'prov-1',
        externalTransactionId: 'ext-1',
        walletId: 'w-1',
        kind: WagerTransactionKind.OPENING,
        amount: Money.fromContract('10.00', 'BRL'),
      }),
    ).toThrow(DomainError);

    expect(() =>
      WagerTransaction.create({
        providerId: 'prov-1',
        externalTransactionId: 'ext-ref-1',
        walletId: 'w-1',
        kind: WagerTransactionKind.REFUND,
        amount: Money.fromContract('10.00', 'BRL'),
      }),
    ).toThrow(DomainError);

    expect(() =>
      WagerTransaction.create({
        providerId: 'prov-1',
        externalTransactionId: 'ext-rb-1',
        walletId: 'w-1',
        kind: WagerTransactionKind.ROLLBACK,
        amount: Money.fromContract('10.00', 'BRL'),
      }),
    ).toThrow(DomainError);
  });

  test('estados terminais (PROCESSED, REJECTED, FAILED) bloqueiam qualquer transição posterior', () => {
    const tx1 = WagerTransaction.create({
      providerId: 'prov-1',
      externalTransactionId: 'ext-1',
      walletId: 'w-1',
      kind: WagerTransactionKind.BET,
      amount: Money.fromContract('10.00', 'BRL'),
    });
    tx1.markProcessed();
    expect(() => tx1.markRejected('ERR', 'fail')).toThrow(InvalidTransactionStateError);
    expect(() => tx1.markPendingReference(new Date())).toThrow(InvalidTransactionStateError);

    const tx2 = WagerTransaction.create({
      providerId: 'prov-1',
      externalTransactionId: 'ext-2',
      walletId: 'w-1',
      kind: WagerTransactionKind.BET,
      amount: Money.fromContract('10.00', 'BRL'),
    });
    tx2.markRejected('INSUFFICIENT_FUNDS', 'no balance');
    expect(() => tx2.markProcessed()).toThrow(InvalidTransactionStateError);

    const tx3 = WagerTransaction.create({
      providerId: 'prov-1',
      externalTransactionId: 'ext-3',
      walletId: 'w-1',
      kind: WagerTransactionKind.BET,
      amount: Money.fromContract('10.00', 'BRL'),
    });
    tx3.markFailed('SYS_ERR', 'crash');
    expect(() => tx3.markProcessed()).toThrow(InvalidTransactionStateError);
  });

  test('calcula payloadHash determinístico independente da ordem das propriedades', () => {
    const hashA = WagerTransaction.computePayloadHash({
      providerId: 'prov-1',
      externalTransactionId: 'ext-100',
      walletId: 'w-1',
      kind: WagerTransactionKind.BET,
      amount: '50.00',
      currency: 'BRL',
      roundId: 'r-1',
    });

    const hashB = WagerTransaction.computePayloadHash({
      roundId: 'r-1',
      currency: 'BRL',
      amount: '50.00',
      kind: WagerTransactionKind.BET,
      walletId: 'w-1',
      externalTransactionId: 'ext-100',
      providerId: 'prov-1',
    });

    expect(hashA).toBe(hashB);
    expect(hashA).toHaveLength(64);
  });
});

// ==================== 4. TESTES DE REGRAS (LOSS, REFUND, ROLLBACK) E IDEMPOTÊNCIA ====================
describe('ProcessWagerTransactionUseCase — Regras de Negócio e Idempotência', () => {
  test('LOSS é marcado como PROCESSED sem alterar saldo nem gerar WalletLedgerEntry', async () => {
    const uow = new InMemoryUnitOfWork();
    const { wallet } = Wallet.open({
      playerId: 'p-1',
      currency: 'BRL',
      initialBalance: Money.fromContract('100.00', 'BRL'),
      openingTransactionId: 'open-1',
    });
    uow.wallets.set(wallet.id, wallet);

    const useCase = new ProcessWagerTransactionUseCase(uow, silentLogger, noopMetrics);

    const result = await useCase.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-loss-1',
      externalTransactionId: 'loss-1',
      walletId: wallet.id,
      kind: WagerTransactionKind.LOSS,
      amount: '25.00',
      currency: 'BRL',
    });

    expect(result.transaction.status).toBe(WagerTransactionStatus.PROCESSED);
    expect(result.observedBalance.amount).toBe('100.00');
    expect(uow.ledgerEntries.length).toBe(0);
    // Deve emitir WagerTransactionProcessed, mas NÃO WalletBalanceChanged
    expect(uow.outboxMessages.map((m) => m.envelope.eventType)).toEqual(['WagerTransactionProcessed']);
  });

  test('REFUND estorna BET e bloqueia segundo REFUND duplicado sobre a mesma aposta', async () => {
    const uow = new InMemoryUnitOfWork();
    const { wallet } = Wallet.open({
      playerId: 'p-2',
      currency: 'BRL',
      initialBalance: Money.fromContract('100.00', 'BRL'),
      openingTransactionId: 'open-2',
    });
    uow.wallets.set(wallet.id, wallet);

    const useCase = new ProcessWagerTransactionUseCase(uow, silentLogger, noopMetrics);

    await useCase.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-bet-1',
      externalTransactionId: 'bet-1',
      walletId: wallet.id,
      kind: WagerTransactionKind.BET,
      amount: '40.00',
      currency: 'BRL',
      roundId: 'round-10',
    });
    expect(wallet.balance.amount).toBe('60.00');

    const refund1 = await useCase.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-ref-1',
      externalTransactionId: 'ref-1',
      walletId: wallet.id,
      kind: WagerTransactionKind.REFUND,
      amount: '40.00',
      currency: 'BRL',
      referenceExternalTransactionId: 'bet-1',
      roundId: 'round-10',
    });
    expect(refund1.transaction.status).toBe(WagerTransactionStatus.PROCESSED);
    expect(wallet.balance.amount).toBe('100.00');

    // Segundo REFUND sobre o mesmo bet-1 deve ser REJECTED com TRANSACTION_ALREADY_REVERSED
    const refund2 = await useCase.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-ref-2',
      externalTransactionId: 'ref-2',
      walletId: wallet.id,
      kind: WagerTransactionKind.REFUND,
      amount: '40.00',
      currency: 'BRL',
      referenceExternalTransactionId: 'bet-1',
      roundId: 'round-10',
    });
    expect(refund2.transaction.status).toBe(WagerTransactionStatus.REJECTED);
    expect(refund2.transaction.failureCode).toBe(LedgerFailureCode.TRANSACTION_ALREADY_REVERSED);
    expect(wallet.balance.amount).toBe('100.00');
  });

  test('ROLLBACK de WIN que resultaria em saldo negativo é rejeitado com ROLLBACK_WOULD_OVERDRAFT', async () => {
    const uow = new InMemoryUnitOfWork();
    const { wallet } = Wallet.open({
      playerId: 'p-3',
      currency: 'BRL',
      initialBalance: Money.fromContract('50.00', 'BRL'),
      openingTransactionId: 'open-3',
    });
    uow.wallets.set(wallet.id, wallet);

    const useCase = new ProcessWagerTransactionUseCase(uow, silentLogger, noopMetrics);

    // 1. WIN de 80.00 -> saldo vai a 130.00
    await useCase.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-win-1',
      externalTransactionId: 'win-1',
      walletId: wallet.id,
      kind: WagerTransactionKind.WIN,
      amount: '80.00',
      currency: 'BRL',
      roundId: 'round-20',
    });
    expect(wallet.balance.amount).toBe('130.00');

    // 2. BET de 100.00 -> saldo vai a 30.00
    await useCase.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-bet-spend',
      externalTransactionId: 'bet-spend',
      walletId: wallet.id,
      kind: WagerTransactionKind.BET,
      amount: '100.00',
      currency: 'BRL',
      roundId: 'round-21',
    });
    expect(wallet.balance.amount).toBe('30.00');

    // 3. ROLLBACK do WIN de 80.00 (exigiria debitar 80.00 tendo apenas 30.00)
    const rollbackRes = await useCase.execute({
      providerId: 'pragmatic',
      idempotencyKey: 'idem-rb-win-1',
      externalTransactionId: 'rb-win-1',
      walletId: wallet.id,
      kind: WagerTransactionKind.ROLLBACK,
      amount: '80.00',
      currency: 'BRL',
      referenceExternalTransactionId: 'win-1',
      roundId: 'round-20',
    });

    expect(rollbackRes.transaction.status).toBe(WagerTransactionStatus.REJECTED);
    expect(rollbackRes.transaction.failureCode).toBe(LedgerFailureCode.ROLLBACK_WOULD_OVERDRAFT);
    expect(wallet.balance.amount).toBe('30.00');
  });

  test('Idempotência: mesmo payloadHash retorna replay sem duplicar débito; payloadHash diferente lança conflito', async () => {
    const uow = new InMemoryUnitOfWork();
    const { wallet } = Wallet.open({
      playerId: 'p-4',
      currency: 'BRL',
      initialBalance: Money.fromContract('100.00', 'BRL'),
      openingTransactionId: 'open-4',
    });
    uow.wallets.set(wallet.id, wallet);

    const useCase = new ProcessWagerTransactionUseCase(uow, silentLogger, noopMetrics);

    const first = await useCase.execute({
      providerId: 'evolution',
      idempotencyKey: 'idem-same-key',
      externalTransactionId: 'bet-evo-1',
      walletId: wallet.id,
      kind: WagerTransactionKind.BET,
      amount: '25.00',
      currency: 'BRL',
    });
    expect(first.idempotentReplay).toBe(false);
    expect(first.observedBalance.amount).toBe('75.00');

    // Replay idêntico
    const replay = await useCase.execute({
      providerId: 'evolution',
      idempotencyKey: 'idem-same-key',
      externalTransactionId: 'bet-evo-1',
      walletId: wallet.id,
      kind: WagerTransactionKind.BET,
      amount: '25.00',
      currency: 'BRL',
    });
    expect(replay.idempotentReplay).toBe(true);
    expect(replay.transaction.id).toBe(first.transaction.id);
    expect(replay.observedBalance.amount).toBe('75.00');
    expect(wallet.balance.amount).toBe('75.00');

    // Mesma chave com valor alterado -> IDEMPOTENCY_PAYLOAD_MISMATCH
    try {
      await useCase.execute({
        providerId: 'evolution',
        idempotencyKey: 'idem-same-key',
        externalTransactionId: 'bet-evo-1',
        walletId: wallet.id,
        kind: WagerTransactionKind.BET,
        amount: '50.00',
        currency: 'BRL',
      });
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(DomainError);
      expect((err as DomainError).code).toBe(LedgerFailureCode.IDEMPOTENCY_PAYLOAD_MISMATCH);
    }
  });
});
