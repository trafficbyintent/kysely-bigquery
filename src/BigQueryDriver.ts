import { type DatabaseConnection, type Driver } from 'kysely';

import { BigQueryConnection } from './BigQueryConnection';

import { type BigQueryDialectConfig } from '.';

/**
 * BigQuery driver implementation for Kysely.
 *
 * Manages connections to BigQuery.
 */
export class BigQueryDriver implements Driver {
  readonly #config: BigQueryDialectConfig;

  constructor(config: BigQueryDialectConfig) {
    this.#config = config;
  }

  init(): Promise<void> {
    return Promise.resolve();
  }

  acquireConnection(): Promise<DatabaseConnection> {
    return Promise.resolve(new BigQueryConnection(this.#config));
  }

  async beginTransaction(conn: BigQueryConnection): Promise<void> {
    return conn.beginTransaction();
  }

  async commitTransaction(conn: BigQueryConnection): Promise<void> {
    return conn.commitTransaction();
  }

  async rollbackTransaction(conn: BigQueryConnection): Promise<void> {
    return conn.rollbackTransaction();
  }

  releaseConnection(_conn: BigQueryConnection): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return Promise.resolve();
  }
}
