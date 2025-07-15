import {
  Kysely,
  DatabaseIntrospector,
  DatabaseMetadata,
  DatabaseMetadataOptions,
  SchemaMetadata,
  TableMetadata,
  sql,
} from 'kysely';
import { BigQueryDialectConfig } from '.';
import { BigQuery } from '@google-cloud/bigquery';
import Bluebird from 'bluebird';

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
    this.#client = new BigQuery(this.#config.options);
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
    _options: DatabaseMetadataOptions = { withInternalKyselyTables: false }
  ): Promise<TableMetadata[]> {
    
    const [datasets] = await this.#client.getDatasets();

    const map: Record<string, TableMetadata> = {};

    await Bluebird.map(datasets, async ({ id }) => {

      const from = sql.id(id ?? '', 'INFORMATION_SCHEMA', 'COLUMNS');

      const rows = await this.#db
        /**
         * Using dynamic schema name which TypeScript cannot validate at compile time.
         * The schema is constructed from dataset IDs retrieved from BigQuery.
         */
        .selectFrom(from as any)
        .selectAll()
        .$castTo<BigQueryInformationSchema>()
        .execute();

      for (const row of rows) {
        const { table_schema, table_name, column_name, is_nullable, data_type, column_default } = row;

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
          isNullable: is_nullable === "YES",
        });

        map[index].columns.push(col);
      }
      
    }, { concurrency: 5 }); /* Limit concurrent requests to avoid overwhelming BigQuery */

    return Object.values(map);
  }

  async getMetadata(
    options?: DatabaseMetadataOptions
  ): Promise<DatabaseMetadata> {
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