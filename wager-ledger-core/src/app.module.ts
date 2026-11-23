import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import mikroOrmConfig from './mikro-orm.config.js';
import {
  InboxMessageOrmEntity,
  OutboxMessageOrmEntity,
  WagerTransactionOrmEntity,
  WalletLedgerEntryOrmEntity,
  WalletOrmEntity,
} from './infrastructure/persistence/entities/orm.entities.js';
import { MikroOrmUnitOfWork } from './infrastructure/persistence/repositories/mikro-orm.uow.js';
import {
  LOGGER_PORT,
  METRICS_PORT,
  UNIT_OF_WORK_PORT,
} from './application/ports/application.ports.js';
import {
  OpenWalletUseCase,
  ProcessWagerTransactionUseCase,
  ReconcileWalletUseCase,
  ReplayPendingReferencesUseCase,
} from './application/use-cases/ledger.use-cases.js';
import { LedgerBackgroundWorkers } from './infrastructure/messaging/workers.service.js';
import { LedgerController } from './infrastructure/http/controllers/ledger.controller.js';
import { PROVIDER_IDENTITY_PORT } from './domain/ports/provider-identity.port.js';
import {
  ExplicitProviderIdentityGuard,
  HeaderProviderIdentityAdapter,
} from './infrastructure/security/explicit-provider-identity.guard.js';
import {
  PrometheusMetricsRegistry,
  StructuredJsonLogger,
} from './infrastructure/observability/observability.service.js';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MikroOrmModule.forRoot(mikroOrmConfig),
    MikroOrmModule.forFeature([
      WalletOrmEntity,
      WagerTransactionOrmEntity,
      WalletLedgerEntryOrmEntity,
      InboxMessageOrmEntity,
      OutboxMessageOrmEntity,
    ]),
  ],
  controllers: [LedgerController],
  providers: [
    StructuredJsonLogger,
    PrometheusMetricsRegistry,
    { provide: LOGGER_PORT, useExisting: StructuredJsonLogger },
    { provide: METRICS_PORT, useExisting: PrometheusMetricsRegistry },
    { provide: UNIT_OF_WORK_PORT, useClass: MikroOrmUnitOfWork },
    { provide: PROVIDER_IDENTITY_PORT, useClass: HeaderProviderIdentityAdapter },
    ExplicitProviderIdentityGuard,
    {
      provide: OpenWalletUseCase,
      useFactory: (uow: MikroOrmUnitOfWork) => new OpenWalletUseCase(uow),
      inject: [UNIT_OF_WORK_PORT],
    },
    {
      provide: ProcessWagerTransactionUseCase,
      useFactory: (
        uow: MikroOrmUnitOfWork,
        logger: StructuredJsonLogger,
        metrics: PrometheusMetricsRegistry,
      ) => new ProcessWagerTransactionUseCase(uow, logger, metrics),
      inject: [UNIT_OF_WORK_PORT, LOGGER_PORT, METRICS_PORT],
    },
    {
      provide: ReconcileWalletUseCase,
      useFactory: (
        uow: MikroOrmUnitOfWork,
        logger: StructuredJsonLogger,
        metrics: PrometheusMetricsRegistry,
      ) => new ReconcileWalletUseCase(uow, logger, metrics),
      inject: [UNIT_OF_WORK_PORT, LOGGER_PORT, METRICS_PORT],
    },
    {
      provide: ReplayPendingReferencesUseCase,
      useFactory: (
        uow: MikroOrmUnitOfWork,
        processWager: ProcessWagerTransactionUseCase,
        logger: StructuredJsonLogger,
      ) => new ReplayPendingReferencesUseCase(uow, processWager, logger),
      inject: [UNIT_OF_WORK_PORT, ProcessWagerTransactionUseCase, LOGGER_PORT],
    },
    {
      provide: LedgerBackgroundWorkers,
      useFactory: (
        uow: MikroOrmUnitOfWork,
        processWager: ProcessWagerTransactionUseCase,
        replay: ReplayPendingReferencesUseCase,
        logger: StructuredJsonLogger,
        metrics: PrometheusMetricsRegistry,
      ) => new LedgerBackgroundWorkers(uow, processWager, replay, logger, metrics),
      inject: [
        UNIT_OF_WORK_PORT,
        ProcessWagerTransactionUseCase,
        ReplayPendingReferencesUseCase,
        LOGGER_PORT,
        METRICS_PORT,
      ],
    },
  ],
})
export class AppModule {}
