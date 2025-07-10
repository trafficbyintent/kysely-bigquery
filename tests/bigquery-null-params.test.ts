import { CompiledQuery, Kysely } from 'kysely';
import { describe, expect, test, vi, beforeEach } from 'vitest';
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

describe('BigQuery Null Parameter Handling', () => {
  let connection: BigQueryConnection;
  let kysely: Kysely<any>;

  beforeEach(() => {
    vi.clearAllMocks();
    connection = new BigQueryConnection({ options: { projectId: 'test-project' } });
    kysely = new Kysely<any>({
      dialect: new BigQueryDialect({ options: { projectId: 'test-project' } }),
    });
  });

  test('should handle null parameters in queries', async () => {
    const testRows = [{ id: 1, name: 'Test', email: null }];
    mockQuery.mockResolvedValue([testRows]);

    const compiledQuery: CompiledQuery = {
      sql: 'SELECT * FROM users WHERE email = ? OR status = ?',
      parameters: [null, 'active'],
    };

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'SELECT * FROM users WHERE email = ? OR status = ?',
      params: [null, 'active'],
      types: ['STRING']
    });
  });

  test('should handle multiple null parameters', async () => {
    mockQuery.mockResolvedValue([[]]);

    const compiledQuery: CompiledQuery = {
      sql: 'INSERT INTO users (name, email, phone) VALUES (?, ?, ?)',
      parameters: ['John', null, null],
    };

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO users (name, email, phone) VALUES (?, ?, ?)',
      params: ['John', null, null],
      types: ['STRING', 'STRING']
    });
  });

  test('should handle query builder with null values', async () => {
    mockQuery.mockResolvedValue([[]]);

    // This simulates what Kysely generates
    const compiledQuery: CompiledQuery = {
      sql: 'UPDATE users SET email = ?, updated_at = ? WHERE id = ?',
      parameters: [null, new Date('2024-01-01'), 123],
    };

    await connection.executeQuery(compiledQuery);

    // Verify the query was called
    expect(mockQuery).toHaveBeenCalled();
    const callArgs = mockQuery.mock.calls[0][0];
    expect(callArgs.params).toEqual([null, new Date('2024-01-01'), 123]);
  });

  test('should provide helpful error message for null parameter type errors', async () => {
    // Simulate BigQuery error for missing null types
    mockQuery.mockRejectedValue(
      new Error('Parameter types must be provided for null values via the \'types\' field in query options.')
    );

    const compiledQuery: CompiledQuery = {
      sql: 'SELECT * FROM users WHERE email = ?',
      parameters: [null],
    };

    await expect(connection.executeQuery(compiledQuery)).rejects.toThrow(
      'BigQuery query failed: Parameter types must be provided for null values'
    );
  });
});