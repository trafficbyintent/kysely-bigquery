import {
  type BigQuery,
  type BigQueryOptions,
  type Dataset,
  type Table,
} from '@google-cloud/bigquery';
import {
  type DatabaseIntrospector,
  type Dialect,
  type Driver,
  type Kysely,
  type QueryCompiler,
} from 'kysely';

import { BigQueryAdapter } from './BigQueryAdapter';
import { BigQueryCompiler } from './BigQueryCompiler';
import { BigQueryDriver } from './BigQueryDriver';
import { BigQueryIntrospector } from './BigQueryIntrospector';

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

  /**
   * Default GCP project ID to prepend to table references.
   *
   * BigQuery supports three-level table names: `project.dataset.table`.
   * Kysely's parser only handles two-level names (`schema.table`), so
   * three-part strings like `'my-project.dataset.table'` lose the table name.
   *
   * Set this to your project ID so you can write `selectFrom('dataset.table')`
   * and the compiler emits `\`project\`.\`dataset\`.\`table\``.
   *
   * Example:
   * ```
   * new BigQueryDialect({
   *   bigquery: client,
   *   defaultProject: 'my-gcp-project',
   * })
   * ```
   */
  defaultProject?: string;
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

  /**
   * Creates a BigQuery adapter for Kysely.
   * @returns A new BigQueryAdapter instance
   */
  createAdapter() {
    return new BigQueryAdapter();
  }

  /**
   * Creates a BigQuery driver for database connections.
   * @returns A new BigQueryDriver instance configured with the dialect settings
   */
  createDriver(): Driver {
    return new BigQueryDriver(this.#config);
  }

  /**
   * Creates a BigQuery query compiler for SQL generation.
   * @returns A new BigQueryCompiler instance that translates Kysely queries to BigQuery SQL
   */
  createQueryCompiler(): QueryCompiler {
    return new BigQueryCompiler(this.#config.defaultProject);
  }

  /**
   * Creates a BigQuery database introspector for schema discovery.
   * @param db - The Kysely database instance
   * @returns A new BigQueryIntrospector instance for examining database schema
   */
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
      if (!config.options.projectId && !(process as any).env['GOOGLE_CLOUD_PROJECT']) {
        throw new Error(
          'BigQuery projectId is required. Provide it in options.projectId or set GOOGLE_CLOUD_PROJECT environment variable.',
        );
      }

    }

    /* Validate bigquery instance if provided */
    if (config.bigquery) {
      const instance = config.bigquery;
      const hasQueryMethod = 'query' in instance && typeof instance.query === 'function';
      const hasCreateQueryStreamMethod =
        'createQueryStream' in instance && typeof instance.createQueryStream === 'function';

      if (!hasQueryMethod || !hasCreateQueryStreamMethod) {
        throw new Error(
          'Invalid bigquery instance provided. It must have query() and createQueryStream() methods.',
        );
      }
    }

    return config;
  }
}
