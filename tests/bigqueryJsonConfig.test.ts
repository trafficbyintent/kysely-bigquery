import { Kysely } from 'kysely';
import { BigQueryDialect } from '../src';
import { describe, expect, test, vi, beforeEach } from 'vitest';

/* Mock the BigQuery client */
const mockQuery = vi.fn();
const mockCreateQueryStream = vi.fn();

vi.mock('@google-cloud/bigquery', () => {
  return {
    BigQuery: class MockBigQuery {
      query = mockQuery;
      createQueryStream = mockCreateQueryStream;
    },
  };
});

interface UserMetadata {
  theme: string;
  notifications: boolean;
}

interface TestTable {
  id: string;
  name: string;
  metadata: UserMetadata;
  settings: Record<string, any>;
  created_at: Date;
}

interface Database {
  'test_dataset.users': TestTable;
}

describe('BigQuery JSON Column Configuration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  test('should automatically serialize JSON when columns are configured', async () => {
    const kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        options: { projectId: 'test-project' },
        jsonColumns: {
          'test_dataset.users': ['metadata', 'settings']
        }
      }),
    });

    const metadata: UserMetadata = {
      theme: 'dark',
      notifications: true
    };

    const settings = {
      language: 'en',
      timezone: 'UTC'
    };

    /* Mock successful response */
    mockQuery.mockResolvedValue([[]]);

    /* This should automatically serialize the JSON objects */
    await kysely
      .insertInto('test_dataset.users')
      .values({
        id: 'test-1',
        name: 'Test User',
        metadata: metadata, // Should be auto-serialized
        settings: settings, // Should be auto-serialized
        created_at: new Date('2024-01-01')
      })
      .execute();

    /* Verify the query was called with serialized JSON */
    expect(mockQuery).toHaveBeenCalled();
    const queryCall = mockQuery.mock.calls[0][0];
    
    /* Check that JSON objects were serialized to strings */
    expect(queryCall.params[2]).toBe(JSON.stringify(metadata));
    expect(queryCall.params[3]).toBe(JSON.stringify(settings));
  });

  test('should not serialize non-JSON columns', async () => {
    const kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        options: { projectId: 'test-project' },
        jsonColumns: {
          'test_dataset.users': ['metadata'] // Only metadata is JSON
        }
      }),
    });

    const metadata: UserMetadata = {
      theme: 'light',
      notifications: false
    };

    mockQuery.mockResolvedValue([[]]);

    await kysely
      .insertInto('test_dataset.users')
      .values({
        id: 'test-2',
        name: 'Test User',
        metadata: metadata, // Should be auto-serialized
        settings: { key: 'value' }, // Should NOT be auto-serialized (not in config)
        created_at: new Date('2024-01-01')
      })
      .execute();

    const queryCall = mockQuery.mock.calls[0][0];
    
    /* metadata should be serialized */
    expect(queryCall.params[2]).toBe(JSON.stringify(metadata));
    /* settings should remain as object (not configured as JSON) */
    expect(queryCall.params[3]).toEqual({ key: 'value' });
  });

  test('should handle UPDATE with JSON columns', async () => {
    const kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        options: { projectId: 'test-project' },
        jsonColumns: {
          'test_dataset.users': ['metadata', 'settings']
        }
      }),
    });

    const newMetadata: UserMetadata = {
      theme: 'auto',
      notifications: true
    };

    mockQuery.mockResolvedValue([[]]);

    await kysely
      .updateTable('test_dataset.users')
      .set({
        metadata: newMetadata,
        settings: { language: 'es' }
      })
      .where('id', '=', 'test-1')
      .execute();

    const queryCall = mockQuery.mock.calls[0][0];
    
    /* Both JSON fields should be serialized */
    expect(queryCall.params[0]).toBe(JSON.stringify(newMetadata));
    expect(queryCall.params[1]).toBe(JSON.stringify({ language: 'es' }));
  });

  test('should parse JSON strings in query results', async () => {
    const kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        options: { projectId: 'test-project' },
        jsonColumns: {
          'test_dataset.users': ['metadata', 'settings']
        }
      }),
    });

    const metadata: UserMetadata = {
      theme: 'dark',
      notifications: true
    };

    /* Mock query response with JSON strings */
    mockQuery.mockResolvedValue([[
      {
        id: 'test-1',
        name: 'Test User',
        metadata: JSON.stringify(metadata),
        settings: JSON.stringify({ language: 'en' }),
        created_at: new Date('2024-01-01')
      }
    ]]);

    const result = await kysely
      .selectFrom('test_dataset.users')
      .selectAll()
      .where('id', '=', 'test-1')
      .executeTakeFirst();

    /* JSON strings should be automatically parsed */
    expect(result?.metadata).toEqual(metadata);
    expect(result?.settings).toEqual({ language: 'en' });
  });

  test('should handle null JSON values', async () => {
    const kysely = new Kysely<Database>({
      dialect: new BigQueryDialect({
        options: { projectId: 'test-project' },
        jsonColumns: {
          'test_dataset.users': ['metadata', 'settings']
        }
      }),
    });

    mockQuery.mockResolvedValue([[]]);

    await kysely
      .insertInto('test_dataset.users')
      .values({
        id: 'test-3',
        name: 'Test User',
        metadata: null as any,
        settings: null as any,
        created_at: new Date('2024-01-01')
      })
      .execute();

    const queryCall = mockQuery.mock.calls[0][0];
    
    /* Null values should remain null */
    expect(queryCall.params[2]).toBeNull();
    expect(queryCall.params[3]).toBeNull();
  });
});