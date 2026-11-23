import { defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';

export default defineConfig({
  host: process.env.DB_HOST ?? 'localhost',
  port: Number(process.env.DB_PORT ?? 5432),
  user: process.env.DB_USER ?? 'app_user',
  password: process.env.DB_PASSWORD ?? 'app_password',
  dbName: process.env.DB_NAME ?? 'wager_db',
  entities: ['dist/**/*.entities.js'],
  entitiesTs: ['src/**/*.entities.ts'],
  migrations: {
    path: 'dist/infrastructure/persistence/migrations',
    pathTs: 'src/infrastructure/persistence/migrations',
  },
  extensions: [Migrator],
  debug: process.env.NODE_ENV !== 'production',
});
