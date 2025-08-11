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

    const compiledQuery = CompiledQuery.raw(
      'SELECT * FROM users WHERE email = ? OR status = ?',
      [null, 'active']
    );

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'SELECT * FROM users WHERE email = ? OR status = ?',
      params: [null, 'active'],
      types: ['STRING', 'STRING']
    });
  });

  test('should handle multiple null parameters', async () => {
    mockQuery.mockResolvedValue([[]]);

    const compiledQuery = CompiledQuery.raw(
      'INSERT INTO users (name, email, phone) VALUES (?, ?, ?)',
      ['John', null, null]
    );

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO users (name, email, phone) VALUES (?, ?, ?)',
      params: ['John', null, null],
      types: ['STRING', 'STRING', 'STRING']
    });
  });

  test('should handle query builder with null values', async () => {
    mockQuery.mockResolvedValue([[]]);

    // This simulates what Kysely generates
    const compiledQuery = CompiledQuery.raw(
      'UPDATE users SET email = ?, updated_at = ? WHERE id = ?',
      [null, new Date('2024-01-01'), 123]
    );

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

    const compiledQuery = CompiledQuery.raw(
      'SELECT * FROM users WHERE email = ?',
      [null]
    );

    await expect(connection.executeQuery(compiledQuery)).rejects.toThrow(
      'BigQuery query failed: Parameter types must be provided for null values'
    );
  });

  test('should handle boolean parameters with type detection', async () => {
    mockQuery.mockResolvedValue([[]]);

    const compiledQuery = CompiledQuery.raw(
      'UPDATE users SET active = ?, verified = ? WHERE id = ?',
      [true, false, 123]
    );

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'UPDATE users SET active = ?, verified = ? WHERE id = ?',
      params: [true, false, 123],
    });
  });

  test('should handle Buffer parameters with type detection', async () => {
    mockQuery.mockResolvedValue([[]]);

    const buffer = Buffer.from('binary data');
    const compiledQuery = CompiledQuery.raw(
      'INSERT INTO files (data, checksum) VALUES (?, ?)',
      [buffer, null]
    );

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO files (data, checksum) VALUES (?, ?)',
      params: [buffer, null],
      types: ['BYTES', 'STRING']
    });
  });

  test('should handle object parameters with type detection', async () => {
    mockQuery.mockResolvedValue([[]]);

    const jsonData = { key: 'value' };
    const arrayData = [1, 2, 3];
    const compiledQuery = CompiledQuery.raw(
      'INSERT INTO data_table (json_col, array_col, null_col) VALUES (?, ?, ?)',
      [jsonData, arrayData, null]
    );

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO data_table (json_col, array_col, null_col) VALUES (?, ?, ?)',
      params: [jsonData, arrayData, null],
      types: ['JSON', 'JSON', 'STRING']
    });
  });

  test('should handle mixed parameter types including all edge cases', async () => {
    mockQuery.mockResolvedValue([[]]);

    const buffer = Buffer.from('test');
    const date = new Date('2024-01-01');
    const object = { nested: true };
    
    const compiledQuery = CompiledQuery.raw(
      'INSERT INTO complex_table VALUES (?, ?, ?, ?, ?, ?, ?)',
      ['string', 42, true, date, buffer, object, null]
    );

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO complex_table VALUES (?, ?, ?, ?, ?, ?, ?)',
      params: ['string', 42, true, date, buffer, object, null],
      types: ['STRING', 'INT64', 'BOOL', 'TIMESTAMP', 'BYTES', 'JSON', 'STRING']
    });
  });

  test('should handle floating point numbers with FLOAT64 type detection', async () => {
    mockQuery.mockResolvedValue([[]]);

    const compiledQuery = CompiledQuery.raw(
      'INSERT INTO measurements (value, price, ratio) VALUES (?, ?, ?)',
      [3.14159, 99.99, null]
    );

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO measurements (value, price, ratio) VALUES (?, ?, ?)',
      params: [3.14159, 99.99, null],
      types: ['FLOAT64', 'FLOAT64', 'STRING']
    });
  });

  test('should distinguish between integers and floats in type detection', async () => {
    mockQuery.mockResolvedValue([[]]);

    const compiledQuery = CompiledQuery.raw(
      'UPDATE stats SET count = ?, average = ?, total = ? WHERE id = ?',
      [100, 75.5, 0.1, null]
    );

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'UPDATE stats SET count = ?, average = ?, total = ? WHERE id = ?',
      params: [100, 75.5, 0.1, null],
      types: ['INT64', 'FLOAT64', 'FLOAT64', 'STRING']
    });
  });
});