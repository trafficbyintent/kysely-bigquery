import { BigQuery } from '@google-cloud/bigquery';
import {
  type Kysely,
  type DatabaseIntrospector,
  type DatabaseMetadata,
  type DatabaseMetadataOptions,
  type SchemaMetadata,
  type TableMetadata,
  sql,
} from 'kysely';

import { type BigQueryDialectConfig } from './BigQueryDialect';

function freeze<T>(obj: T): Readonly<T> {
  return Object.freeze(obj);
}

/**
 * Database introspector for BigQuery.
 *
 * Provides schema metadata by querying BigQuery's INFORMATION_SCHEMA.
 */
export class BigQueryIntrospector implements DatabaseIntrospector {
  readonly #db: Kysely<unknown>;
  readonly #config: BigQueryDialectConfig;
  readonly #client: BigQuery;

  constructor(db: Kysely<unknown>, config: BigQueryDialectConfig) {
    this.#db = db;
    this.#config = config;
    this.#client = this.#resolveBigQueryClient();
  }

  /**
   * Resolves the BigQuery client from the dialect config.
   *
   * Uses the provided bigquery instance if it has getDatasets (i.e., is a BigQuery instance).
   * Falls back to creating a new BigQuery client from options.
   */
  #resolveBigQueryClient(): BigQuery {
    if (
      this.#config.bigquery &&
      'getDatasets' in this.#config.bigquery &&
      typeof this.#config.bigquery.getDatasets === 'function'
    ) {
      return this.#config.bigquery;
    }
    return new BigQuery(this.#config.options);
  }

  async getSchemas(): Promise<SchemaMetadata[]> {
    const [datasets] = await this.#client.getDatasets();

    return datasets.map((dataset) => {
      return freeze({
        name: dataset.id ?? '',
      });
    });
  }

  async getTables(
    _options: DatabaseMetadataOptions = { withInternalKyselyTables: false },
  ): Promise<TableMetadata[]> {
    const [datasets] = await this.#client.getDatasets();

    const map: Record<string, TableMetadata> = {};

    await Promise.all(
      datasets.map(async ({ id }) => {
        const from = sql.id(id ?? '', 'INFORMATION_SCHEMA', 'COLUMNS');

        /* Using dynamic schema name which TypeScript cannot validate at compile time */
        /* The schema is constructed from dataset IDs retrieved from BigQuery */
        /* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access */
        const query = this.#db.selectFrom(from as any);
        const rows = (await (query as any).selectAll().execute()) as BigQueryInformationSchema[];
        /* eslint-enable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access */

        for (const row of rows) {
          const { table_schema, table_name, column_name, is_nullable, data_type, column_default } =
            row;

          const index = `${table_schema}.${table_name}`;

          if (!map[index]) {
            map[index] = {
              isView: false,
              name: table_name,
              schema: table_schema,
              columns: [],
            };
          }

          const col = freeze({
            name: column_name,
            dataType: data_type,
            hasDefaultValue: column_default !== null && column_default !== 'NULL',
            isAutoIncrementing: false,
            isNullable: is_nullable === 'YES',
          });

          map[index].columns.push(col);
        }
      }),
    ); /* Limit concurrent requests to avoid overwhelming BigQuery */

    return Object.values(map);
  }

  async getMetadata(options?: DatabaseMetadataOptions): Promise<DatabaseMetadata> {
    return {
      tables: await this.getTables(options),
    };
  }
}

/**
 * BigQuery INFORMATION_SCHEMA.COLUMNS table structure.
 */
interface BigQueryInformationSchema {
  table_catalog: string;
  table_schema: string;
  table_name: string;
  column_name: string;
  ordinal_position: string;
  is_nullable: string;
  data_type: string;
  is_generated: string;
  is_hidden: string;
  is_system_defined: string;
  is_partitioning_column: string;
  collation_name: string;
  column_default: string;
}
