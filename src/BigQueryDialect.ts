import {BigQuery, BigQueryOptions, Dataset, Table} from '@google-cloud/bigquery';
import {
  DatabaseIntrospector,
  Dialect,
  Driver,
  Kysely,
  MysqlAdapter,
  QueryCompiler,
} from 'kysely';
import {BigQueryDriver} from './BigQueryDriver';
import {BigQueryIntrospector} from './BigQueryIntrospector';
import {BigQueryQueryCompiler} from './BigQueryQueryCompiler';

export interface BigQueryDialectConfig {
  options?: BigQueryOptions;
  bigquery?: BigQuery | Dataset | Table;
}

export class BigQueryDialect implements Dialect {
  readonly #config: BigQueryDialectConfig;

  constructor(config?: BigQueryDialectConfig) {
    this.#config = config ?? {};
  }

  createAdapter() {
    return new MysqlAdapter();
  }

  createDriver(): Driver {
    return new BigQueryDriver(this.#config);
  }

  createQueryCompiler(): QueryCompiler {
    return new BigQueryQueryCompiler();
  }

  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new BigQueryIntrospector(db, this.#config);
  }
}

