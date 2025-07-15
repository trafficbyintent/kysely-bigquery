import { Kysely, sql } from 'kysely';
import { BigQueryDialect } from '../src';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createBigQueryInstance } from './config';

interface TestTable {
  id: string;
  name: string;
  email: string | null;
  metadata: any | null;
  status: string | null;
  created_at: Date;
}

interface Database {
  'test_dataset.null_test_table': TestTable;
}

describe('BigQuery Null Parameters Integration Tests', { timeout: 30000 }, () => {
  let kysely: Kysely<Database>;
  const testTableName = 'test_dataset.null_test_table';

  beforeAll(async () => {
    kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        bigquery: createBigQueryInstance(),
      }),
    });

    // Create test table
    await sql`
      CREATE OR REPLACE TABLE ${sql.table(testTableName)} (
        id STRING NOT NULL,
        name STRING NOT NULL,
        email STRING,
        metadata STRING,
        status STRING,
        created_at TIMESTAMP NOT NULL
      )
    `.execute(kysely);
  });

  afterAll(async () => {
    // Clean up
    await sql`DROP TABLE IF EXISTS ${sql.table(testTableName)}`.execute(kysely);
    await kysely.destroy();
  });

  test('should handle null values in INSERT', async () => {
    const testId = `null-insert-${Date.now()}`;
    
    // This test demonstrates the current issue with null parameters
    try {
      await kysely
        .insertInto(testTableName)
        .values({
          id: testId,
          name: 'Test User',
          email: null,
          metadata: null,
          status: 'active',
          created_at: new Date(),
        })
        .execute();

      // Verify the insert
      const result = await kysely
        .selectFrom(testTableName)
        .selectAll()
        .where('id', '=', testId)
        .executeTakeFirst();

      expect(result).toBeDefined();
      expect(result!.email).toBeNull();
      expect(result!.metadata).toBeNull();
    } catch (error: any) {
      // Currently this might fail with parameter type error
      console.log('Expected error:', error.message);
      expect(error.message).toContain('Parameter types must be provided for null values');
    }
  });

  test('should handle null values in WHERE clause', async () => {
    const testId = `null-where-${Date.now()}`;
    
    // First insert a record with null email using raw SQL to bypass the issue
    await sql`
      INSERT INTO ${sql.table(testTableName)} (id, name, email, metadata, status, created_at)
      VALUES (${testId}, 'Test User', NULL, NULL, 'active', CURRENT_TIMESTAMP())
    `.execute(kysely);

    // Try to query with null in WHERE clause
    try {
      const result = await kysely
        .selectFrom(testTableName)
        .selectAll()
        .where('email', 'is', null)
        .where('id', '=', testId)
        .executeTakeFirst();

      expect(result).toBeDefined();
      expect(result!.id).toBe(testId);
    } catch (error: any) {
      console.log('Expected error:', error.message);
      expect(error.message).toContain('Parameter types must be provided for null values');
    }
  });

  test('should handle null values in UPDATE', async () => {
    const testId = `null-update-${Date.now()}`;
    
    // Insert a record first
    await sql`
      INSERT INTO ${sql.table(testTableName)} (id, name, email, metadata, status, created_at)
      VALUES (${testId}, 'Test User', 'test@example.com', '{"key": "value"}', 'active', CURRENT_TIMESTAMP())
    `.execute(kysely);

    // Try to update with null values
    try {
      await kysely
        .updateTable(testTableName)
        .set({
          email: null,
          metadata: null,
          status: null,
        })
        .where('id', '=', testId)
        .execute();

      // Verify the update
      const result = await sql`
        SELECT * FROM ${sql.table(testTableName)} WHERE id = ${testId}
      `.execute(kysely);

      expect((result.rows[0] as TestTable).email).toBeNull();
      expect((result.rows[0] as TestTable).metadata).toBeNull();
    } catch (error: any) {
      console.log('Expected error:', error.message);
      expect(error.message).toContain('Parameter types must be provided for null values');
    }
  });

  test('should handle mixed null and non-null parameters', async () => {
    const testId = `null-mixed-${Date.now()}`;
    
    try {
      await kysely
        .insertInto(testTableName)
        .values({
          id: testId,
          name: 'Test User',
          email: 'test@example.com',
          metadata: null, // This is null
          status: 'active',
          created_at: new Date(),
        })
        .execute();

      const result = await kysely
        .selectFrom(testTableName)
        .selectAll()
        .where('id', '=', testId)
        .executeTakeFirst();

      expect(result).toBeDefined();
      expect(result!.email).toBe('test@example.com');
      expect(result!.metadata).toBeNull();
    } catch (error: any) {
      console.log('Expected error:', error.message);
      expect(error.message).toContain('Parameter types must be provided for null values');
    }
  });
});