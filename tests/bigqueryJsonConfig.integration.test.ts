import { Kysely, sql } from 'kysely';
import { BigQueryDialect } from '../src';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createBigQueryInstance } from './config';

interface UserMetadata {
  tags: string[];
  settings: {
    theme: string;
    notifications: boolean;
  };
}

interface TestTable {
  id: string;
  name: string;
  metadata: UserMetadata;
  preferences: Record<string, any>;
  created_at: Date;
}

interface Database {
  'test_dataset.json_config_test_table': TestTable;
}

describe('BigQuery JSON Configuration Integration Tests', { timeout: 30000 }, () => {
  let kysely: Kysely<Database>;
  const testTableName = 'test_dataset.json_config_test_table';

  beforeAll(async () => {
    kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        bigquery: createBigQueryInstance(),
        // Configure JSON columns
        jsonColumns: {
          'test_dataset.json_config_test_table': ['metadata', 'preferences']
        }
      }),
    });

    // Create test table - BigQuery uses STRING for JSON data
    await sql`
      CREATE OR REPLACE TABLE ${sql.table(testTableName)} (
        id STRING NOT NULL,
        name STRING NOT NULL,
        metadata STRING,
        preferences STRING,
        created_at TIMESTAMP NOT NULL
      )
    `.execute(kysely);
  });

  afterAll(async () => {
    // Clean up
    await sql`DROP TABLE IF EXISTS ${sql.table(testTableName)}`.execute(kysely);
    await kysely.destroy();
  });

  test('should automatically serialize JSON objects with configured columns', async () => {
    const testId = `json-auto-${Date.now()}`;
    const metadata: UserMetadata = {
      tags: ['test', 'bigquery', 'json'],
      settings: {
        theme: 'dark',
        notifications: true,
      },
    };

    // Should work without manual JSON.stringify
    await kysely
      .insertInto('test_dataset.json_config_test_table')
      .values({
        id: testId,
        name: 'Test User',
        metadata: metadata, // Auto-serialized
        preferences: { language: 'en', timezone: 'UTC' }, // Auto-serialized
        created_at: new Date(),
      })
      .execute();

    // Verify the data was inserted and parsed correctly
    const result = await kysely
      .selectFrom('test_dataset.json_config_test_table')
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result).toBeDefined();
    expect(result!.metadata).toEqual(metadata);
    expect(result!.preferences).toEqual({ language: 'en', timezone: 'UTC' });
  });

  test('should handle UPDATE with automatic JSON serialization', async () => {
    const testId = `json-update-auto-${Date.now()}`;
    
    // Insert initial data
    await kysely
      .insertInto('test_dataset.json_config_test_table')
      .values({
        id: testId,
        name: 'Test User',
        metadata: {
          tags: ['initial'],
          settings: { theme: 'light', notifications: false }
        },
        preferences: { language: 'en' },
        created_at: new Date(),
      })
      .execute();

    // Update with new JSON data
    const newMetadata: UserMetadata = {
      tags: ['updated', 'test'],
      settings: {
        theme: 'dark',
        notifications: true,
      },
    };

    await kysely
      .updateTable('test_dataset.json_config_test_table')
      .set({
        metadata: newMetadata, // Auto-serialized
        preferences: { language: 'es', timezone: 'PST' }, // Auto-serialized
      })
      .where('id', '=', testId)
      .execute();

    // Verify the update
    const result = await kysely
      .selectFrom('test_dataset.json_config_test_table')
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result!.metadata).toEqual(newMetadata);
    expect(result!.preferences).toEqual({ language: 'es', timezone: 'PST' });
  });

  test('should handle null JSON values', async () => {
    const testId = `json-null-${Date.now()}`;

    await kysely
      .insertInto('test_dataset.json_config_test_table')
      .values({
        id: testId,
        name: 'Test User',
        metadata: null as any,
        preferences: null as any,
        created_at: new Date(),
      })
      .execute();

    const result = await kysely
      .selectFrom('test_dataset.json_config_test_table')
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result!.metadata).toBeNull();
    expect(result!.preferences).toBeNull();
  });

  test('should handle complex nested JSON structures', async () => {
    const testId = `json-complex-${Date.now()}`;
    const complexData = {
      nested: {
        deeply: {
          nested: {
            value: 'test',
            array: [1, 2, 3],
            bool: true,
          },
        },
      },
      mixedArray: ['string', 123, true, { key: 'value' }],
    };

    await kysely
      .insertInto('test_dataset.json_config_test_table')
      .values({
        id: testId,
        name: 'Test User',
        metadata: { tags: [], settings: { theme: 'light', notifications: false } },
        preferences: complexData, // Auto-serialized
        created_at: new Date(),
      })
      .execute();

    const result = await kysely
      .selectFrom('test_dataset.json_config_test_table')
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result!.preferences).toEqual(complexData);
  });
});