import { BigQuery, BigQueryOptions, Dataset, Table } from '@google-cloud/bigquery';
import {
  DatabaseIntrospector,
  Dialect,
  Driver,
  Kysely,
  QueryCompiler,
} from 'kysely';
import {BigQueryAdapter} from './bigQueryAdapter';
import {BigQueryDriver} from './bigQueryDriver';
import {BigQueryIntrospector} from './bigQueryIntrospector';
import {BigQueryCompiler} from './bigQueryCompiler';

/**
 * Configuration options for BigQuery dialect.
 */
export interface BigQueryDialectConfig {
  /**
   * BigQuery client options.
   */
  options?: BigQueryOptions;
  
  /**
   * Existing BigQuery, Dataset, or Table instance to use.
   * If provided, this will be used instead of creating a new client.
   */
  bigquery?: BigQuery | Dataset | Table;
  
  /**
   * Optional mapping of table names to their JSON column names.
   * This enables automatic JSON serialization for these columns.
   * 
   * Example:
   * ```
   * {
   *   'dataset.users': ['metadata', 'settings'],
   *   'dataset.products': ['specifications']
   * }
   * ```
   */
  jsonColumns?: Record<string, string[]>;
}

/**
 * BigQuery dialect for Kysely.
 * 
 * This dialect allows you to use Kysely with Google BigQuery.
 */
export class BigQueryDialect implements Dialect {
  readonly #config: BigQueryDialectConfig;

  constructor(config?: BigQueryDialectConfig) {
    this.#config = this.#validateConfig(config ?? {});
  }

  createAdapter() {
    return new BigQueryAdapter();
  }

  createDriver(): Driver {
    return new BigQueryDriver(this.#config);
  }

  createQueryCompiler(): QueryCompiler {
    return new BigQueryCompiler();
  }

  createIntrospector(db: Kysely<unknown>): DatabaseIntrospector {
    return new BigQueryIntrospector(db, this.#config);
  }

  #validateConfig(config: BigQueryDialectConfig): BigQueryDialectConfig {
    /* Validate mutually exclusive options */
    if (config.options && config.bigquery) {
      throw new Error(
        'Cannot provide both "options" and "bigquery" in BigQueryDialectConfig. ' +
        'Use either "options" to create a new client or "bigquery" to use an existing instance.',
      );
    }

    /* Validate BigQuery options if provided */
    if (config.options) {
      /* Ensure projectId is provided when using options */
      if (!config.options.projectId && !process.env['GOOGLE_CLOUD_PROJECT']) {
        throw new Error(
          'BigQuery projectId is required. Provide it in options.projectId or set GOOGLE_CLOUD_PROJECT environment variable.',
        );
      }

      /* Validate credentials */
      if (!config.options.keyFilename && !config.options.credentials && !process.env['GOOGLE_APPLICATION_CREDENTIALS']) {
        /* Authentication will fall back to Application Default Credentials */
      }
    }

    /* Validate bigquery instance if provided */
    if (config.bigquery) {
      const instance = config.bigquery;
      const hasQueryMethod = typeof (instance as any).query === 'function';
      const hasCreateQueryStreamMethod = typeof (instance as any).createQueryStream === 'function';
      
      if (!hasQueryMethod || !hasCreateQueryStreamMethod) {
        throw new Error(
          'Invalid bigquery instance provided. It must have query() and createQueryStream() methods.',
        );
      }
    }

    return config;
  }
}

