import { Kysely, sql } from 'kysely';
import { BigQueryDialect } from '../src';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createBigQueryInstance } from './config';

interface JsonTestTable {
  id: string;
  name: string;
  metadata: any; // JSON column
  settings: any; // JSON column
  tags: string[]; // ARRAY column
  details: { // STRUCT column
    category: string;
    priority: number;
  };
  created_at: Date;
}

interface Database {
  'test_dataset.json_qb_test_table': JsonTestTable;
}

describe('BigQuery Native JSON Columns Integration Tests', { timeout: 30000 }, () => {
  let kysely: Kysely<Database>;
  const testTableName = 'test_dataset.json_qb_test_table';

  beforeAll(async () => {
    kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        bigquery: createBigQueryInstance(),
        // Don't register native JSON columns for automatic serialization
        // BigQuery handles native JSON columns automatically
      }),
    });

    // Create test table with JSON columns
    await sql`
      CREATE OR REPLACE TABLE ${sql.table(testTableName)} (
        id STRING NOT NULL,
        name STRING NOT NULL,
        metadata JSON,
        settings JSON,
        tags ARRAY<STRING>,
        details STRUCT<category STRING, priority INT64>,
        created_at TIMESTAMP NOT NULL
      )
    `.execute(kysely);
  });

  afterAll(async () => {
    // Clean up
    await sql`DROP TABLE IF EXISTS ${sql.table(testTableName)}`.execute(kysely);
    await kysely.destroy();
  });

  test('should handle native JSON columns in INSERT using JSON literals', async () => {
    const testId = `json-insert-${Date.now()}`;
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

    // For BigQuery JSON columns, we need to use raw SQL with PARSE_JSON
    // The automatic serialization helps ensure JSON is properly stringified,
    // but BigQuery still requires PARSE_JSON for native JSON columns
    await sql`
      INSERT INTO ${sql.table(testTableName)} 
      (id, name, metadata, settings, tags, details, created_at)
      VALUES (
        ${testId},
        'Test User',
        PARSE_JSON(${JSON.stringify(metadata)}),
        PARSE_JSON(${JSON.stringify(settings)}),
        ${['tag1', 'tag2']},
        ${sql`STRUCT('test' as category, 1 as priority)`},
        ${new Date()}
      )
    `.execute(kysely);

    // Verify the data was inserted correctly
    const result = await kysely
      .selectFrom(testTableName)
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result).toBeDefined();
    expect(result!.metadata).toEqual(metadata);
    expect(result!.settings).toEqual(settings);
    expect(result!.tags).toEqual(['tag1', 'tag2']);
    expect(result!.details).toEqual({ category: 'test', priority: 1 });
  });

  test('should handle native JSON columns in UPDATE using JSON literals', async () => {
    const testId = `json-update-${Date.now()}`;
    
    // First insert a record using raw SQL for native JSON columns
    await sql`
      INSERT INTO ${sql.table(testTableName)} 
      (id, name, metadata, settings, tags, details, created_at)
      VALUES (
        ${testId},
        'Test User',
        JSON '{"version": "1.0"}',
        JSON '{"theme": "light"}',
        ['original'],
        STRUCT('initial' as category, 0 as priority),
        ${new Date()}
      )
    `.execute(kysely);

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

    // For native JSON columns, we need to use raw SQL with JSON literals
    await sql`
      UPDATE ${sql.table(testTableName)}
      SET 
        metadata = JSON ${sql.lit(JSON.stringify(newMetadata))},
        settings = JSON ${sql.lit(JSON.stringify(newSettings))},
        tags = ['updated', 'modified']
      WHERE id = ${testId}
    `.execute(kysely);

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

  test('should handle null JSON values', async () => {
    const testId = `json-null-${Date.now()}`;

    await sql`
      INSERT INTO ${sql.table(testTableName)} 
      (id, name, metadata, settings, tags, details, created_at)
      VALUES (
        ${testId},
        'Test User',
        NULL,
        JSON '{"theme": "default"}',
        [],
        STRUCT('test' as category, 0 as priority),
        ${new Date()}
      )
    `.execute(kysely);

    const result = await kysely
      .selectFrom(testTableName)
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result).toBeDefined();
    expect(result!.metadata).toBeNull();
    expect(result!.settings).toEqual({ theme: 'default' });
  });

  test('should handle complex nested JSON structures', async () => {
    const testId = `json-complex-${Date.now()}`;
    const complexData = {
      users: [
        { id: 1, name: 'User 1', active: true },
        { id: 2, name: 'User 2', active: false }
      ],
      configuration: {
        database: {
          host: 'localhost',
          port: 5432,
          credentials: {
            username: 'admin',
            encrypted: true
          }
        },
        features: {
          'feature-1': { enabled: true, config: { level: 'high' } },
          'feature-2': { enabled: false, config: null }
        }
      },
      metadata: {
        created: new Date().toISOString(),
        version: 1,
        tags: ['production', 'critical']
      }
    };

    await sql`
      INSERT INTO ${sql.table(testTableName)} 
      (id, name, metadata, settings, tags, details, created_at)
      VALUES (
        ${testId},
        'Complex Test',
        JSON ${sql.lit(JSON.stringify(complexData))},
        NULL,
        ['complex'],
        STRUCT('advanced' as category, 100 as priority),
        ${new Date()}
      )
    `.execute(kysely);

    const result = await kysely
      .selectFrom(testTableName)
      .selectAll()
      .where('id', '=', testId)
      .executeTakeFirst();

    expect(result).toBeDefined();
    expect(result!.metadata).toEqual(complexData);
  });

  test('should work with WHERE clauses containing JSON data', async () => {
    const testId = `json-where-${Date.now()}`;
    
    await sql`
      INSERT INTO ${sql.table(testTableName)} 
      (id, name, metadata, settings, tags, details, created_at)
      VALUES (
        ${testId},
        'Where Test',
        JSON '{"type": "special", "value": 42}',
        JSON '{"enabled": true}',
        ['searchable'],
        STRUCT('search' as category, 1 as priority),
        ${new Date()}
      )
    `.execute(kysely);

    // Query using JSON_VALUE
    const result = await sql`
      SELECT * FROM ${sql.table(testTableName)}
      WHERE id = ${testId}
      AND JSON_VALUE(metadata, '$.type') = 'special'
    `.execute(kysely);

    expect(result.rows).toHaveLength(1);
    expect((result.rows[0] as JsonTestTable).id).toBe(testId);
  });
});