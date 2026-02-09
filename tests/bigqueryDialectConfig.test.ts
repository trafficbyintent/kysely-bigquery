import { Kysely } from 'kysely';
import { describe, expect, test, vi, beforeEach } from 'vitest';

/* Mock BigQuery before importing src files */
const mockQuery = vi.fn();
const mockCreateQueryStream = vi.fn();
const mockGetDatasets = vi.fn();

vi.mock('@google-cloud/bigquery', () => {
  class MockBigQuery {
    query = mockQuery;
    createQueryStream = mockCreateQueryStream;
    getDatasets = mockGetDatasets;
  }

  class MockDataset {
    query = mockQuery;
    createQueryStream = mockCreateQueryStream;
  }

  class MockTable {
    query = mockQuery;
    createQueryStream = mockCreateQueryStream;
  }

  return {
    BigQuery: MockBigQuery,
    Dataset: MockDataset,
    Table: MockTable,
  };
});

import { BigQueryDialect } from '../src';

describe('BigQueryDialect Configuration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  test('creates dialect with options', () => {
    const dialect = new BigQueryDialect({
      options: { projectId: 'test-project' },
    });

    expect(dialect).toBeDefined();
    expect(dialect.createAdapter()).toBeDefined();
    expect(dialect.createDriver()).toBeDefined();
    expect(dialect.createQueryCompiler()).toBeDefined();
  });

  test('creates dialect with existing BigQuery instance', () => {
    const mockBigQuery = {
      query: vi.fn(),
      createQueryStream: vi.fn(),
      getDatasets: vi.fn(),
    };
    
    const dialect = new BigQueryDialect({
      bigquery: mockBigQuery as any,
    });

    expect(dialect).toBeDefined();
    const driver = dialect.createDriver();
    expect(driver).toBeDefined();
  });

  test('creates dialect with existing Dataset instance', () => {
    const mockDataset = {
      query: vi.fn(),
      createQueryStream: vi.fn(),
    };
    
    const dialect = new BigQueryDialect({
      bigquery: mockDataset as any,
    });

    expect(dialect).toBeDefined();
    const driver = dialect.createDriver();
    expect(driver).toBeDefined();
  });

  test('creates dialect with existing Table instance', () => {
    const mockTable = {
      query: vi.fn(),
      createQueryStream: vi.fn(),
    };
    
    const dialect = new BigQueryDialect({
      bigquery: mockTable as any,
    });

    expect(dialect).toBeDefined();
    const driver = dialect.createDriver();
    expect(driver).toBeDefined();
  });

  test('throws error when both options and bigquery are provided', () => {
    const mockBigQuery = {
      query: vi.fn(),
      createQueryStream: vi.fn(),
    };

    expect(() => {
      new BigQueryDialect({
        options: { projectId: 'test-project' },
        bigquery: mockBigQuery as any,
      });
    }).toThrow(
      'Cannot provide both "options" and "bigquery" in BigQueryDialectConfig. Use either "options" to create a new client or "bigquery" to use an existing instance.'
    );
  });

  test('validates bigquery instance has required methods', () => {
    const invalidInstance = { someMethod: vi.fn() };

    expect(() => {
      new BigQueryDialect({
        bigquery: invalidInstance as any,
      });
    }).toThrow(
      'Invalid bigquery instance provided. It must have query() and createQueryStream() methods.'
    );
  });

  test('validates bigquery instance has createQueryStream method', () => {
    const invalidInstance = { 
      query: vi.fn(),
      /* Missing createQueryStream */
    };

    expect(() => {
      new BigQueryDialect({
        bigquery: invalidInstance as any,
      });
    }).toThrow(
      'Invalid bigquery instance provided. It must have query() and createQueryStream() methods.'
    );
  });

  test('creates working Kysely instance with BigQuery instance', async () => {
    mockQuery.mockResolvedValue([[]]);

    const mockBigQuery = {
      query: mockQuery,
      createQueryStream: mockCreateQueryStream,
    };

    const db = new Kysely<any>({
      dialect: new BigQueryDialect({ bigquery: mockBigQuery as any }),
    });

    /* Should be able to compile and execute queries */
    const query = db.selectFrom('users').selectAll();
    const compiled = query.compile();
    
    expect(compiled.sql).toBe('select * from `users`');
    expect(compiled.parameters).toEqual([]);

    /* Execute the query */
    await query.execute();
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'select * from `users`',
      params: [],
      parseJSON: true,
    });
  });

  test('creates working Kysely instance with Dataset instance', async () => {
    mockQuery.mockResolvedValue([[]]);

    const mockDataset = {
      query: mockQuery,
      createQueryStream: mockCreateQueryStream,
    };

    const db = new Kysely<any>({
      dialect: new BigQueryDialect({ bigquery: mockDataset as any }),
    });

    const query = db.selectFrom('users').selectAll();
    await query.execute();

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'select * from `users`',
      params: [],
      parseJSON: true,
    });
  });

  test('creates working Kysely instance with Table instance', async () => {
    mockQuery.mockResolvedValue([[]]);

    const mockTable = {
      query: mockQuery,
      createQueryStream: mockCreateQueryStream,
    };

    const db = new Kysely<any>({
      dialect: new BigQueryDialect({ bigquery: mockTable as any }),
    });

    const query = db.selectFrom('users').selectAll();
    await query.execute();

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'select * from `users`',
      params: [],
      parseJSON: true,
    });
  });

  test('introspector uses provided bigquery instance', () => {
    const mockBigQuery = {
      query: vi.fn(),
      createQueryStream: vi.fn(),
      getDatasets: vi.fn(),
    };
    
    const dialect = new BigQueryDialect({ bigquery: mockBigQuery as any });
    
    const db = new Kysely<any>({ dialect });
    const introspector = dialect.createIntrospector(db);
    
    expect(introspector).toBeDefined();
  });

  test('throws error when projectId is missing', () => {
    /* Remove GOOGLE_CLOUD_PROJECT if it exists */
    const originalEnv = process.env.GOOGLE_CLOUD_PROJECT;
    delete process.env.GOOGLE_CLOUD_PROJECT;

    try {
      expect(() => {
        new BigQueryDialect({
          options: {
            /* Missing projectId */
            keyFilename: '/path/to/key.json'
          } as any,
        });
      }).toThrow(
        'BigQuery projectId is required. Provide it in options.projectId or set GOOGLE_CLOUD_PROJECT environment variable.'
      );
    } finally {
      /* Restore original env */
      if (originalEnv !== undefined) {
        process.env.GOOGLE_CLOUD_PROJECT = originalEnv;
      }
    }
  });
});