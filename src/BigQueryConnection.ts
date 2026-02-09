import { BigQuery, type Dataset, type Query, type Table } from '@google-cloud/bigquery';
import { type CompiledQuery, type DatabaseConnection, type QueryResult } from 'kysely';

import { JsonColumnDetector } from './jsonColumnDetector';

import { type BigQueryDialectConfig } from './BigQueryDialect';

/**
 * BigQuery database connection implementation for Kysely.
 *
 * Handles query execution and streaming for BigQuery.
 */
export class BigQueryConnection implements DatabaseConnection {
  readonly #client: BigQuery | Dataset | Table;
  readonly #jsonDetector: JsonColumnDetector;
  readonly #jsonColumnNames: Set<string>;

  constructor(config: BigQueryDialectConfig) {
    this.#client = config.bigquery ?? new BigQuery(config.options);
    this.#jsonDetector = new JsonColumnDetector();

    /* Register known JSON columns if provided in config */
    if (config.jsonColumns) {
      for (const [tableName, columns] of Object.entries(config.jsonColumns)) {
        this.#jsonDetector.registerJsonColumns(tableName, columns);
      }
    }

    this.#jsonColumnNames = this.#jsonDetector.getRegisteredJsonColumnNames();
  }

  /**
   * Executes a compiled query against BigQuery.
   * @param compiledQuery - The compiled query with SQL and parameters
   * @returns A promise that resolves to the query results
   * @throws Error if the query fails or if null parameters are not properly typed
   */
  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    try {
      const params = [...compiledQuery.parameters];

      /* Process parameters to handle nulls and JSON serialization */
      const processedParams = this.#jsonDetector.processParameters(compiledQuery, params);

      const options: Query = {
        query: compiledQuery.sql,
        params: processedParams,
      };

      /* BigQuery needs types array for ALL parameters when there are null parameters */
      if (processedParams.some((p) => p === null)) {
        options.types = this.#inferParamTypes(processedParams);
      }

      const [rows] = await this.#client.query(options);

      const processedRows = Array.isArray(rows)
        ? rows.map((row) => this.#processResultRow(row as Record<string, unknown>))
        : [];

      return {
        rows: processedRows as O[],
      };
    } catch (error) {
      /* Provide more helpful error messages */
      if (error instanceof Error) {
        if (
          error.message.includes('Parameter types must be provided for null values') ||
          error.message.includes('Incorrect number of parameter types provided')
        ) {
          throw new Error(
            `BigQuery query failed: ${error.message}\n` +
              'Hint: The BigQuery dialect now automatically handles null parameters. ' +
              "If you're still seeing this error, please report it as a bug.",
          );
        }
        throw new Error(`BigQuery query failed: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Begins a transaction (not supported by BigQuery).
   * @returns A rejected promise as BigQuery doesn't support transactions
   */
  beginTransaction(): Promise<void> {
    return Promise.reject(new Error('Transactions are not supported.'));
  }

  /**
   * Commits a transaction (not supported by BigQuery).
   * @returns A rejected promise as BigQuery doesn't support transactions
   */
  commitTransaction(): Promise<void> {
    return Promise.reject(new Error('Transactions are not supported.'));
  }

  /**
   * Rolls back a transaction (not supported by BigQuery).
   * @returns A rejected promise as BigQuery doesn't support transactions
   */
  rollbackTransaction(): Promise<void> {
    return Promise.reject(new Error('Transactions are not supported.'));
  }

  /**
   * Streams query results for handling large datasets.
   * @param compiledQuery - The compiled query with SQL and parameters
   * @param _chunkSize - Chunk size parameter (currently unused)
   * @returns An async iterator that yields query results in batches
   */
  async *streamQuery<O>(
    compiledQuery: CompiledQuery,
    _chunkSize: number,
  ): AsyncIterableIterator<QueryResult<O>> {
    const params = [...compiledQuery.parameters];

    /* Process parameters to handle nulls and JSON serialization */
    const processedParams = this.#jsonDetector.processParameters(compiledQuery, params);

    const options: Query = {
      query: compiledQuery.sql,
      params: processedParams,
    };

    /* BigQuery needs types array for ALL parameters when there are null parameters */
    if (processedParams.some((p) => p === null)) {
      options.types = this.#inferParamTypes(processedParams);
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let stream: any;
    try {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      stream = this.#client.createQueryStream(options);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`BigQuery stream query failed: ${error.message}`);
      }
      throw error;
    }

    try {
      for await (const row of stream) {
        yield {
          rows: [this.#processResultRow(row as Record<string, unknown>) as O],
        };
      }
    } catch (error) {
      /* Handle stream errors */
      if (error instanceof Error) {
        throw new Error(`BigQuery stream error: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Infers BigQuery parameter types from JavaScript values.
   * Required when any parameter is null because BigQuery needs explicit types.
   */
  #inferParamTypes(params: readonly unknown[]): string[] {
    return params.map((param) => {
      if (param === null) {
        return 'STRING';
      }
      if (typeof param === 'number') {
        return Number.isInteger(param) ? 'INT64' : 'FLOAT64';
      }
      if (typeof param === 'boolean') {
        return 'BOOL';
      }
      if (param instanceof Date) {
        return 'TIMESTAMP';
      }
      if (param instanceof Buffer) {
        return 'BYTES';
      }
      if (typeof param === 'object') {
        return 'JSON';
      }
      return 'STRING';
    });
  }

  /**
   * Processes a single result row, parsing registered JSON columns.
   * Only columns registered via jsonColumns config are parsed.
   */
  #processResultRow(row: Record<string, unknown>): Record<string, unknown> {
    if (this.#jsonColumnNames.size === 0) {
      return row;
    }

    const processedRow: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      if (this.#jsonColumnNames.has(key) && typeof value === 'string' && value.length > 0) {
        try {
          processedRow[key] = JSON.parse(value);
        } catch {
          processedRow[key] = value;
        }
      } else {
        processedRow[key] = value;
      }
    }
    return processedRow;
  }
}
