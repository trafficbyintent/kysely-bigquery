import { DatabaseConnection, Driver } from 'kysely';

import { BigQueryDialectConfig } from '.';
import { BigQueryConnection } from './bigQueryConnection';

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

  async init(): Promise<void> {}

  async acquireConnection(): Promise<DatabaseConnection> {
    return new BigQueryConnection(this.#config);
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

  async releaseConnection(_conn: BigQueryConnection): Promise<void> {}

  async destroy(): Promise<void> {}
}