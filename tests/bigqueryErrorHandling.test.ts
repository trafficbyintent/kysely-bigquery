import { CompiledQuery } from 'kysely';
import { describe, expect, test, vi, beforeEach } from 'vitest';

import { BigQueryConnection, BigQueryDriver } from '../src';

/* Mock BigQuery client */
const mockQuery = vi.fn();
const mockCreateQueryStream = vi.fn();

vi.mock('@google-cloud/bigquery', () => {
  class MockBigQuery {
    query = mockQuery;
    createQueryStream = mockCreateQueryStream;
  }
  return { BigQuery: MockBigQuery };
});

describe('BigQuery Error Handling', () => {
  let connection: BigQueryConnection;
  let driver: BigQueryDriver;

  beforeEach(() => {
    vi.clearAllMocks();
    connection = new BigQueryConnection({ options: { projectId: 'test-project' } });
    driver = new BigQueryDriver({ options: { projectId: 'test-project' } });
  });

  describe('Query Execution Errors', () => {
    test('handles BigQuery query errors with message', async () => {
      const testError = new Error('Table not found: test_table');
      mockQuery.mockRejectedValue(testError);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM test_table', []);

      await expect(connection.executeQuery(compiledQuery)).rejects.toThrow(
        'BigQuery query failed: Table not found: test_table'
      );
    });

    test('handles non-Error exceptions in query', async () => {
      mockQuery.mockRejectedValue('String error');

      const compiledQuery = CompiledQuery.raw('SELECT * FROM test_table', []);

      await expect(connection.executeQuery(compiledQuery)).rejects.toBe('String error');
    });

    test('handles query timeout errors', async () => {
      const timeoutError = new Error('Query exceeded timeout');
      timeoutError.name = 'TimeoutError';
      mockQuery.mockRejectedValue(timeoutError);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM large_table', []);

      await expect(connection.executeQuery(compiledQuery)).rejects.toThrow(
        'BigQuery query failed: Query exceeded timeout'
      );
    });

    test('handles permission errors', async () => {
      const permissionError = new Error('User does not have permission to query table');
      mockQuery.mockRejectedValue(permissionError);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM restricted_table', []);

      await expect(connection.executeQuery(compiledQuery)).rejects.toThrow(
        'BigQuery query failed: User does not have permission to query table'
      );
    });
  });

  describe('Transaction Errors', () => {
    test('beginTransaction throws error', async () => {
      await expect(connection.beginTransaction()).rejects.toThrow(
        'Transactions are not supported.'
      );
    });

    test('commitTransaction throws error', async () => {
      await expect(connection.commitTransaction()).rejects.toThrow(
        'Transactions are not supported.'
      );
    });

    test('rollbackTransaction throws error', async () => {
      await expect(connection.rollbackTransaction()).rejects.toThrow(
        'Transactions are not supported.'
      );
    });
  });

  describe('Driver Methods', () => {
    test('init does not throw', async () => {
      await expect(driver.init()).resolves.toBeUndefined();
    });

    test('acquireConnection returns a connection', async () => {
      const conn = await driver.acquireConnection();
      expect(conn).toBeDefined();
      expect(conn).toBeInstanceOf(BigQueryConnection);
    });

    test('beginTransaction throws error', async () => {
      const conn = await driver.acquireConnection() as BigQueryConnection;
      await expect(driver.beginTransaction(conn)).rejects.toThrow(
        'Transactions are not supported.'
      );
    });

    test('commitTransaction throws error', async () => {
      const conn = await driver.acquireConnection() as BigQueryConnection;
      await expect(driver.commitTransaction(conn)).rejects.toThrow(
        'Transactions are not supported.'
      );
    });

    test('rollbackTransaction throws error', async () => {
      const conn = await driver.acquireConnection() as BigQueryConnection;
      await expect(driver.rollbackTransaction(conn)).rejects.toThrow(
        'Transactions are not supported.'
      );
    });

    test('releaseConnection does not throw', async () => {
      const conn = await driver.acquireConnection() as BigQueryConnection;
      await expect(driver.releaseConnection(conn)).resolves.toBeUndefined();
    });

    test('destroy does not throw', async () => {
      await expect(driver.destroy()).resolves.toBeUndefined();
    });
  });

  describe('Query Result Edge Cases', () => {
    test('handles null query results', async () => {
      mockQuery.mockResolvedValue([null]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM empty_table', []);

      const result = await connection.executeQuery(compiledQuery);
      expect(result.rows).toEqual([]);
    });

    test('handles undefined query results', async () => {
      mockQuery.mockResolvedValue([undefined]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM empty_table', []);

      const result = await connection.executeQuery(compiledQuery);
      expect(result.rows).toEqual([]);
    });

    test('handles non-array query results', async () => {
      mockQuery.mockResolvedValue([{ notAnArray: true }]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM weird_table', []);

      const result = await connection.executeQuery(compiledQuery);
      expect(result.rows).toEqual([]);
    });

    test('handles empty array results', async () => {
      mockQuery.mockResolvedValue([[]]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM empty_table', []);

      const result = await connection.executeQuery(compiledQuery);
      expect(result.rows).toEqual([]);
    });
  });

  describe('Invalid SQL Handling', () => {
    test('handles syntax errors in SQL', async () => {
      const syntaxError = new Error('Syntax error: Unexpected keyword SELECT at [1:1]');
      mockQuery.mockRejectedValue(syntaxError);

      const compiledQuery = CompiledQuery.raw('SELECT SELECT * FROM table', []);

      await expect(connection.executeQuery(compiledQuery)).rejects.toThrow(
        'BigQuery query failed: Syntax error: Unexpected keyword SELECT at [1:1]'
      );
    });

    test('handles invalid table references', async () => {
      const invalidRefError = new Error('Table name "invalid..table" missing dataset');
      mockQuery.mockRejectedValue(invalidRefError);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM invalid..table', []);

      await expect(connection.executeQuery(compiledQuery)).rejects.toThrow(
        'BigQuery query failed: Table name "invalid..table" missing dataset'
      );
    });
  });
});