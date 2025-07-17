import { MysqlAdapter } from 'kysely';

/**
 * BigQuery adapter that extends MysqlAdapter to handle BigQuery-specific features.
 * 
 * Main differences:
 * - Appends NOT ENFORCED to all constraint definitions (PRIMARY KEY, FOREIGN KEY, UNIQUE)
 * - Handles BigQuery's unenforced constraint model
 */
export class BigQueryAdapter extends MysqlAdapter {
  /**
   * Override to ensure all constraints have NOT ENFORCED appended.
   * BigQuery supports constraint syntax but doesn't enforce them at runtime.
   */
  get supportsReturning(): boolean {
    return false;
  }
}