import { CompiledQuery, Kysely } from 'kysely';
import { describe, expect, test, vi, beforeEach } from 'vitest';
import { Readable } from 'stream';

import { BigQueryDialect, BigQueryConnection } from '../src';

// Mock the BigQuery client
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

    // Create a mock readable stream
    const mockStream = new Readable({
      objectMode: true,
      read() {
        // Push test rows one by one
        if (testRows.length > 0) {
          this.push(testRows.shift());
        } else {
          this.push(null); // End the stream
        }
      },
    });

    mockCreateQueryStream.mockResolvedValue(mockStream);

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

    mockCreateQueryStream.mockResolvedValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users WHERE 1=0', []);

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 10);

    for await (const result of stream) {
      results.push(result);
    }

    expect(results).toHaveLength(0);
  });

  test('streamQuery handles stream creation errors', async () => {
    mockCreateQueryStream.mockRejectedValue(new Error('Failed to create stream'));

    const compiledQuery = CompiledQuery.raw('SELECT * FROM invalid_table', []);

    const stream = connection.streamQuery(compiledQuery, 1);

    await expect(async () => {
      for await (const _ of stream) {
        // Should throw before yielding any results
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
          // Emit error on next tick
          process.nextTick(() => {
            this.destroy(new Error('Stream error'));
          });
        }
      },
    });

    mockCreateQueryStream.mockResolvedValue(mockStream);

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);

    const results: any[] = [];
    const stream = connection.streamQuery(compiledQuery, 1);

    await expect(async () => {
      for await (const result of stream) {
        results.push(result);
      }
    }).rejects.toThrow('BigQuery stream error: Stream error');

    // Should have collected one result before the error
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

    mockCreateQueryStream.mockResolvedValue(mockStream);

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
    mockCreateQueryStream.mockRejectedValue('String error');

    const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);

    const stream = connection.streamQuery(compiledQuery, 1);

    await expect(async () => {
      for await (const _ of stream) {
        // Should throw
      }
    }).rejects.toBe('String error');
  });

  test('stream method in Kysely query builder', async () => {
    const kysely = new Kysely<any>({
      dialect: new BigQueryDialect(),
    });

    // The stream method should be available on queries
    const query = kysely.selectFrom('users').selectAll();
    expect(query.stream).toBeDefined();
    expect(typeof query.stream).toBe('function');
  });
});