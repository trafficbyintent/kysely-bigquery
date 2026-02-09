import { CompiledQuery, Kysely } from 'kysely';
import { describe, expect, test, vi, beforeEach } from 'vitest';
import { Readable } from 'stream';

import { BigQueryDialect, BigQueryConnection } from '../src';

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

describe('BigQuery Streaming', () => {
  let connection: BigQueryConnection;

  beforeEach(() => {
    vi.clearAllMocks();
    connection = new BigQueryConnection({ options: { projectId: 'test-project' } });
  });

  test('streamQuery returns rows one by one', async () => {
    const testRows = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
    ];

    /* Create a mock readable stream */
    const mockStream = new Readable({
      objectMode: true,
      read() {
        /* Push test rows one by one */
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null); // End the stream
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 1);

    for await (const result of stream) {
      results.push(result);
    }

    expect(results).toHaveLength(3);
    expect(results[0]).toEqual({ rows: [{ id: 1, name: 'Alice' }] });
    expect(results[1]).toEqual({ rows: [{ id: 2, name: 'Bob' }] });
    expect(results[2]).toEqual({ rows: [{ id: 3, name: 'Charlie' }] });

    expect(mockCreateQueryStream).toHaveBeenCalledWith({
      query: 'SELECT * FROM users',
      params: [],
    });
  });

  test('streamQuery handles empty results', async () => {
    const mockStream = new Readable({
      objectMode: true,
      read() {
        this.push(null); // Empty stream
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users WHERE 1=0', []);

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 10);

    for await (const result of stream) {
      results.push(result);
    }

    expect(results).toHaveLength(0);
  });

  test('streamQuery handles stream creation errors', async () => {
    mockCreateQueryStream.mockImplementation(() => {
      throw new Error('Failed to create stream');
    });

    const compiledQuery = CompiledQuery.raw('SELECT * FROM invalid_table', []);

    const stream = connection.streamQuery(compiledQuery, 1);

    await expect(async () => {
      for await (const _ of stream) {
        /* Should throw before yielding any results */
      }
    }).rejects.toThrow('BigQuery stream query failed: Failed to create stream');
  });

  test('streamQuery handles stream errors during iteration', async () => {
    let errorEmitted = false;
    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (!errorEmitted) {
          this.push({ id: 1, name: 'Alice' });
          errorEmitted = true;
          /* Emit error on next tick */
          process.nextTick(() => {
            this.destroy(new Error('Stream error'));
          });
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 1);

    await expect(async () => {
      for await (const result of stream) {
        results.push(result);
      }
    }).rejects.toThrow('BigQuery stream error: Stream error');

    /* Should have collected one result before the error */
    expect(results).toHaveLength(1);
  });

  test('streamQuery with parameters', async () => {
    const mockStream = new Readable({
      objectMode: true,
      read() {
        this.push({ id: 1, name: 'Alice', age: 25 });
        this.push(null);
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users WHERE age > ?', [21]);

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 1);

    for await (const result of stream) {
      results.push(result);
    }

    expect(mockCreateQueryStream).toHaveBeenCalledWith({
      query: 'SELECT * FROM users WHERE age > ?',
      params: [21],
    });
    expect(results).toHaveLength(1);
  });

  test('streamQuery handles non-Error exceptions', async () => {
    mockCreateQueryStream.mockImplementation(() => {
      throw 'String error';
    });

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);

    const stream = connection.streamQuery(compiledQuery, 1);

    await expect(async () => {
      for await (const _ of stream) {
        /* Should throw */
      }
    }).rejects.toBe('String error');
  });

  test('stream method in Kysely query builder', async () => {
    const kysely = new Kysely<any>({
      dialect: new BigQueryDialect(),
    });

    /* The stream method should be available on queries */
    const query = kysely.selectFrom('users').selectAll();
    expect(query.stream).toBeDefined();
    expect(typeof query.stream).toBe('function');
  });

  test('streamQuery does not auto-parse JSON without registration', async () => {
    const testRows = [
      { id: 1, metadata: '{"role": "admin"}' },
      { id: 2, metadata: '[1, 2, 3]' },
    ];

    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null);
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);
    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 1);

    for await (const result of stream) {
      results.push(result);
    }

    /* Without jsonColumns config, JSON strings are returned as-is */
    expect(results[0].rows[0].metadata).toBe('{"role": "admin"}');
    expect(results[1].rows[0].metadata).toBe('[1, 2, 3]');
  });

  test('streamQuery parses registered JSON columns', async () => {
    const registeredConnection = new BigQueryConnection({
      options: { projectId: 'test-project' },
      jsonColumns: { 'dataset.users': ['metadata'] },
    });

    const testRows = [
      { id: 1, metadata: '{"role": "admin", "permissions": ["read", "write"]}' },
      { id: 2, metadata: '[1, 2, 3]' },
      { id: 3, metadata: 'not json' },
    ];

    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null);
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);
    const results: any[] = [];
    const stream = registeredConnection.streamQuery(compiledQuery, 1);

    for await (const result of stream) {
      results.push(result);
    }

    /* Registered JSON columns are parsed */
    expect(results[0].rows[0].metadata).toEqual({ role: 'admin', permissions: ['read', 'write'] });
    expect(results[1].rows[0].metadata).toEqual([1, 2, 3]);
    /* Non-JSON-looking strings are returned as-is even for registered columns */
    expect(results[2].rows[0].metadata).toBe('not json');
  });

  test('streamQuery handles JSON parse errors gracefully', async () => {
    const testRows = [
      { id: 1, data: '{invalid json' }, /* Malformed JSON */
      { id: 2, data: '{truncated: ' }, /* Incomplete JSON */
    ];

    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null);
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);
    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 1);

    for await (const result of stream) {
      results.push(result);
    }

    /* Malformed JSON should remain as strings */
    expect(results[0].rows[0].data).toBe('{invalid json');
    expect(results[1].rows[0].data).toBe('{truncated: ');
  });

  test('streamQuery handles non-Error exceptions in stream', async () => {
    let pushed = false;
    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (!pushed) {
          this.push({ id: 1, name: 'Test' });
          pushed = true;
          process.nextTick(() => {
            /* Emit non-Error exception */
            this.destroy('String error in stream' as any);
          });
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);
    const stream = connection.streamQuery(compiledQuery, 1);

    await expect(async () => {
      for await (const _ of stream) {
        /* Should throw */
      }
    }).rejects.toBe('String error in stream');
  });

  test('streamQuery handles null parameters with type detection', async () => {
    const testRows = [{ id: 1, name: 'Test' }];

    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null);
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw(
      'SELECT * FROM users WHERE email = ? OR status = ?',
      [null, 'active']
    );

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 10);

    for await (const result of stream) {
      results.push(result);
    }

    expect(mockCreateQueryStream).toHaveBeenCalledWith({
      query: 'SELECT * FROM users WHERE email = ? OR status = ?',
      params: [null, 'active'],
      types: ['STRING', 'STRING']
    });
  });

  test('streamQuery handles all parameter types with null values', async () => {
    const testRows = [{ id: 1 }];

    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null);
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const buffer = Buffer.from('test');
    const date = new Date('2024-01-01');
    const compiledQuery = CompiledQuery.raw(
      'INSERT INTO test_table VALUES (?, ?, ?, ?, ?, ?, ?)',
      ['string', 42, true, date, buffer, { key: 'value' }, null]
    );

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 10);

    for await (const result of stream) {
      results.push(result);
    }

    expect(mockCreateQueryStream).toHaveBeenCalledWith({
      query: 'INSERT INTO test_table VALUES (?, ?, ?, ?, ?, ?, ?)',
      params: ['string', 42, true, date, buffer, { key: 'value' }, null],
      types: ['STRING', 'INT64', 'BOOL', 'TIMESTAMP', 'BYTES', 'STRING', 'STRING']
    });
  });

  test('streamQuery handles floating point numbers with FLOAT64 type detection', async () => {
    const testRows = [{ id: 1 }];

    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null);
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw(
      'SELECT * FROM measurements WHERE value > ? AND price < ? AND ratio = ?',
      [3.14159, 99.99, null]
    );

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 10);

    for await (const result of stream) {
      results.push(result);
    }

    expect(mockCreateQueryStream).toHaveBeenCalledWith({
      query: 'SELECT * FROM measurements WHERE value > ? AND price < ? AND ratio = ?',
      params: [3.14159, 99.99, null],
      types: ['FLOAT64', 'FLOAT64', 'STRING']
    });
  });

  test('streamQuery handles JSON parse errors in registered columns', async () => {
    const registeredConnection = new BigQueryConnection({
      options: { projectId: 'test-project' },
      jsonColumns: { 'dataset.users': ['metadata', 'settings'] },
    });

    const testRows = [
      {
        id: 1,
        metadata: '{"valid": "json"}',
        settings: '{"malformed": json}',
      },
    ];

    const mockStream = new Readable({
      objectMode: true,
      read() {
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null);
        }
      },
    });

    mockCreateQueryStream.mockReturnValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);

    const results: any[] = [];
    const stream = registeredConnection.streamQuery(compiledQuery, 10);

    for await (const result of stream) {
      results.push(result);
    }

    expect(results).toHaveLength(1);
    const row = results[0].rows[0];

    /* Valid JSON strings should be parsed */
    expect(row.metadata).toEqual({ valid: 'json' });

    /* Malformed JSON should remain as string */
    expect(row.settings).toBe('{"malformed": json}');
  });
});