import { Kysely, sql } from 'kysely';
import { BigQueryDialect } from '../src';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createBigQueryInstance } from './config';

interface JsonStringTable {
  id: string;
  name: string;
  metadata: any; // STRING column that stores JSON
  settings: any; // STRING column that stores JSON
  tags: string[]; // ARRAY column
  created_at: Date;
}

interface Database {
  'test_dataset.json_string_table': JsonStringTable;
}

describe('BigQuery JSON with STRING columns Integration Tests', { timeout: 30000 }, () => {
  let kysely: Kysely<Database>;
  const testTableName = 'test_dataset.json_string_table';

  beforeAll(async () => {
    kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        bigquery: createBigQueryInstance(),
        // Register JSON columns for automatic serialization
        jsonColumns: {
          'test_dataset.json_string_table': ['metadata', 'settings']
        }
      }),
    });

    // Create test table with STRING columns for JSON data
    // This is the most common pattern for JSON in BigQuery
    await sql`
      CREATE OR REPLACE TABLE ${sql.table(testTableName)} (
        id STRING NOT NULL,
        name STRING NOT NULL,
        metadata STRING, -- Will store JSON as string
        settings STRING, -- Will store JSON as string
        tags ARRAY<STRING>,
        created_at TIMESTAMP NOT NULL
      )
    `.execute(kysely);
  });

  afterAll(async () => {
    // Clean up
    await sql`DROP TABLE IF EXISTS ${sql.table(testTableName)}`.execute(kysely);
    await kysely.destroy();
  });

  test('should automatically stringify JSON for registered STRING columns in INSERT', async () => {
    const testId = `json-string-insert-${Date.now()}`;
    const metadata = {
      version: '1.0',
      features: ['feature1', 'feature2'],
      config: {
        enabled: true,
        level: 5
      }
    };
    const settings = {
      theme: 'dark',
      notifications: {
        email: true,
        push: false
      }
    };

    // Using Kysely query builder - JSON is automatically stringified for registered columns
    await kysely
      .insertInto(testTableName)
      .values({
        id: testId,
        name: 'Test User',
        metadata: metadata, // Automatically stringified because registered as JSON column
        settings: settings, // Automatically stringified because registered as JSON column
        tags: ['tag1', 'tag2'], // Array is NOT stringified
        created_at: new Date(),
      })
      .execute();

    // Verify the data was inserted correctly
    const result = await kysely
      .selectFrom(testTableName)
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result).toBeDefined();
    // The automatic parsing in BigQueryConnection parses JSON strings back to objects
    expect(result!.metadata).toEqual(metadata);
    expect(result!.settings).toEqual(settings);
    expect(result!.tags).toEqual(['tag1', 'tag2']);
  });

  test('should automatically stringify JSON for registered STRING columns in UPDATE', async () => {
    const testId = `json-string-update-${Date.now()}`;
    
    // First insert a record
    await kysely
      .insertInto(testTableName)
      .values({
        id: testId,
        name: 'Test User',
        metadata: { version: '1.0' },
        settings: { theme: 'light' },
        tags: ['original'],
        created_at: new Date(),
      })
      .execute();

    // Update with new JSON data
    const newMetadata = {
      version: '2.0',
      updatedAt: new Date().toISOString(),
      changes: ['update1', 'update2']
    };
    const newSettings = {
      theme: 'dark',
      language: 'en',
      features: {
        beta: true
      }
    };

    await kysely
      .updateTable(testTableName)
      .set({
        metadata: newMetadata, // Automatically stringified
        settings: newSettings, // Automatically stringified
        tags: ['updated', 'modified'], // Array is NOT stringified
      })
      .where('id', '=', testId)
      .execute();

    // Verify the update
    const result = await kysely
      .selectFrom(testTableName)
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result).toBeDefined();
    expect(result!.metadata).toEqual(newMetadata);
    expect(result!.settings).toEqual(newSettings);
    expect(result!.tags).toEqual(['updated', 'modified']);
  });

  test('should require explicit registration for JSON columns', async () => {
    // Create a new Kysely instance without jsonColumns config
    const kyselyNoConfig = new Kysely<Database>({
      dialect: new BigQueryDialect({
        bigquery: createBigQueryInstance(),
        // No jsonColumns config - no automatic serialization
      }),
    });

    const testId = `json-string-manual-${Date.now()}`;
    const metadata = { manual: true, info: 'Must stringify manually without config' };

    try {
      // Without registration, objects must be manually stringified
      await kyselyNoConfig
        .insertInto(testTableName)
        .values({
          id: testId,
          name: 'Manual Test',
          metadata: JSON.stringify(metadata), // Must manually stringify
          settings: JSON.stringify({ manual: true }), // Must manually stringify
          tags: ['manual'],
          created_at: new Date(),
        })
        .execute();

      const result = await kyselyNoConfig
        .selectFrom(testTableName)
        .selectAll()
        .where('id', '=', testId)
        .executeTakeFirst();

      expect(result).toBeDefined();
      // Automatic parsing still works on SELECT
      expect(result!.metadata).toEqual(metadata);
    } finally {
      await kyselyNoConfig.destroy();
    }
  });

  test('should handle querying JSON data with JSON functions', async () => {
    const testId = `json-string-query-${Date.now()}`;
    const metadata = {
      type: 'premium',
      features: {
        storage: 1000,
        users: 50
      }
    };
    
    await kysely
      .insertInto(testTableName)
      .values({
        id: testId,
        name: 'Query Test',
        metadata: metadata,
        settings: { plan: 'pro' },
        tags: ['queryable'],
        created_at: new Date(),
      })
      .execute();

    // Query using JSON_VALUE on the STRING column
    const result = await sql<{type: string; storage: string}>`
      SELECT 
        JSON_VALUE(metadata, '$.type') as type,
        JSON_VALUE(metadata, '$.features.storage') as storage
      FROM ${sql.table(testTableName)}
      WHERE id = ${testId}
    `.execute(kysely);

    expect(result.rows[0].type).toBe('premium');
    expect(result.rows[0].storage).toBe('1000');
  });
});