import { MysqlAdapter } from 'kysely';

/**
 * BigQuery adapter that extends MysqlAdapter.
 *
 * Disables the RETURNING clause, which BigQuery does not support.
 */
export class BigQueryAdapter extends MysqlAdapter {
  public get supportsReturning(): boolean {
    return false;
  }
}
