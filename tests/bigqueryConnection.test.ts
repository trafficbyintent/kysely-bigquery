import { CompiledQuery } from 'kysely';
import { describe, expect, test, vi, beforeEach } from 'vitest';

import { BigQueryConnection } from '../src';

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

describe('BigQueryConnection', () => {
  let connection: BigQueryConnection;

  beforeEach(() => {
    vi.clearAllMocks();
    connection = new BigQueryConnection({ options: { projectId: 'test-project' } });
  });

  describe('executeQuery', () => {
    test('handles empty result set', async () => {
      /* Mock BigQuery to return empty results */
      mockQuery.mockResolvedValue([[]]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM users WHERE 1=0', []);
      const result = await connection.executeQuery(compiledQuery);

      expect(result.rows).toEqual([]);
      expect(mockQuery).toHaveBeenCalledWith({
        query: 'SELECT * FROM users WHERE 1=0',
        params: [],
      });
    });

    test('handles null/undefined rows from BigQuery', async () => {
      /* Mock BigQuery to return null/undefined */
      mockQuery.mockResolvedValue([null]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM deleted_table', []);
      const result = await connection.executeQuery(compiledQuery);

      expect(result.rows).toEqual([]);
    });

    test('handles JSON parsing in results', async () => {
      const mockRows = [
        { id: 1, metadata: '{"role": "admin"}', settings: '[1,2,3]' },
        { id: 2, metadata: 'not json', settings: '{"invalid": }' },
      ];

      mockQuery.mockResolvedValue([mockRows]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);
      const result = await connection.executeQuery(compiledQuery);

      /* Valid JSON should be parsed */
      expect(result.rows[0].metadata).toEqual({ role: 'admin' });
      expect(result.rows[0].settings).toEqual([1, 2, 3]);

      /* Invalid JSON should remain as strings */
      expect(result.rows[1].metadata).toBe('not json');
      expect(result.rows[1].settings).toBe('{"invalid": }');
    });

    test('handles empty strings in JSON columns', async () => {
      const mockRows = [
        { id: 1, metadata: '', settings: ' ' },
      ];

      mockQuery.mockResolvedValue([mockRows]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM users', []);
      const result = await connection.executeQuery(compiledQuery);

      /* Empty strings should not be parsed */
      expect(result.rows[0].metadata).toBe('');
      expect(result.rows[0].settings).toBe(' ');
    });

    test('preserves non-string values', async () => {
      const mockRows = [
        { id: 1, count: 42, active: true, ratio: 3.14, nothing: null },
      ];

      mockQuery.mockResolvedValue([mockRows]);

      const compiledQuery = CompiledQuery.raw('SELECT * FROM stats', []);
      const result = await connection.executeQuery(compiledQuery);

      expect(result.rows[0]).toEqual({
        id: 1,
        count: 42,
        active: true,
        ratio: 3.14,
        nothing: null,
      });
    });
  });
});