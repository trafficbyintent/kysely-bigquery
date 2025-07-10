import {
  CompiledQuery,
  DatabaseConnection,
  QueryResult,
} from 'kysely';

import { BigQuery, Dataset, Table } from '@google-cloud/bigquery';
import { BigQueryDialectConfig } from '.';


/**
 * BigQuery database connection implementation for Kysely.
 * 
 * Handles query execution and streaming for BigQuery.
 */
export class BigQueryConnection implements DatabaseConnection {
  #client: BigQuery | Dataset | Table;

  constructor(config: BigQueryDialectConfig) {
    this.#client = config.bigquery ?? new BigQuery(config.options);
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    try {
      const options = {
        query: compiledQuery.sql,
        params: [...compiledQuery.parameters],
      };

      const [rows] = await this.#client.query(options);

      return {
        insertId: undefined,
        rows: Array.isArray(rows) ? rows as O[] : [],
        numAffectedRows: undefined,
        /**
         * @deprecated numUpdatedOrDeletedRows is deprecated in kysely >= 0.23.
         * Kept for backward compatibility.
         */
        numUpdatedOrDeletedRows: undefined,
      };
    } catch (error) {
      // Re-throw with more context
      if (error instanceof Error) {
        throw new Error(`BigQuery query failed: ${error.message}`);
      }
      throw error;
    }
  }

  async beginTransaction() {
    throw new Error('Transactions are not supported.');
  }

  async commitTransaction() {
    throw new Error('Transactions are not supported.');
  }

  async rollbackTransaction() {
    throw new Error('Transactions are not supported.');
  }

  async *streamQuery<O>(compiledQuery: CompiledQuery, chunkSize: number): AsyncIterableIterator<QueryResult<O>> {
    const options = {
      query: compiledQuery.sql,
      params: [...compiledQuery.parameters],
    };

    let stream;
    try {
      stream = await this.#client.createQueryStream(options);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`BigQuery stream query failed: ${error.message}`);
      }
      throw error;
    }

    try {
      for await (const row of stream) {
        yield {
          rows: [row],
        };
      }
    } catch (error) {
      // Handle stream errors
      if (error instanceof Error) {
        throw new Error(`BigQuery stream error: ${error.message}`);
      }
      throw error;
    }
  }
}