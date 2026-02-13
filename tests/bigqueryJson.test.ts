import { CompiledQuery, Kysely } from 'kysely';
import { describe, expect, test, vi, beforeEach } from 'vitest';
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

describe('BigQuery JSON Field Handling', () => {
  let connection: BigQueryConnection;
  let kysely: Kysely<any>;

  beforeEach(() => {
    vi.clearAllMocks();
    connection = new BigQueryConnection({ options: { projectId: 'test-project' } });
    kysely = new Kysely<any>({
      dialect: new BigQueryDialect({ options: { projectId: 'test-project' } }),
    });
  });

  test('should handle JSON fields in INSERT queries', async () => {
    mockQuery.mockResolvedValue([[]]);

    const metadata = { 
      tags: ['test', 'bigquery'], 
      settings: { theme: 'dark', notifications: true } 
    };

    const compiledQuery: CompiledQuery = {
      sql: 'INSERT INTO users (name, metadata) VALUES (?, ?)',
      parameters: ['John', metadata],
      query: {} as any
    };

    await connection.executeQuery(compiledQuery);

    /* BigQuery connection should pass objects as-is
       JSON serialization should be handled at application level for JSON type fields */
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO users (name, metadata) VALUES (?, ?)',
      params: ['John', metadata],
      parseJSON: true,
    });
  });

  test('should handle JSON fields in UPDATE queries', async () => {
    mockQuery.mockResolvedValue([[]]);

    const newSettings = { 
      preferences: { 
        language: 'en', 
        timezone: 'UTC' 
      } 
    };

    const compiledQuery: CompiledQuery = {
      sql: 'UPDATE users SET settings = ? WHERE id = ?',
      parameters: [newSettings, 1],
      query: {} as any
    };

    await connection.executeQuery(compiledQuery);

    /* BigQuery connection should pass objects as-is */
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'UPDATE users SET settings = ? WHERE id = ?',
      params: [newSettings, 1],
      parseJSON: true,
    });
  });

  test('should handle null JSON fields', async () => {
    mockQuery.mockResolvedValue([[]]);

    const compiledQuery: CompiledQuery = {
      sql: 'UPDATE users SET metadata = ? WHERE id = ?',
      parameters: [null, 1],
      query: {} as any
    };

    await connection.executeQuery(compiledQuery);

    expect(mockQuery).toHaveBeenCalledWith({
      query: 'UPDATE users SET metadata = ? WHERE id = ?',
      params: [null, 1],
      types: ['STRING', 'INT64'],
      parseJSON: true,
    });
  });

  test('should not auto-parse JSON fields in SELECT results without registration', async () => {
    const mockRows = [{
      id: 1,
      name: 'John',
      metadata: '{"tags":["test","bigquery"],"settings":{"theme":"dark"}}',
    }];

    mockQuery.mockResolvedValue([mockRows]);

    const compiledQuery: CompiledQuery = {
      sql: 'SELECT * FROM users WHERE id = ?',
      parameters: [1],
      query: {} as any
    };

    const result = await connection.executeQuery<any>(compiledQuery);

    /* Without jsonColumns config, JSON strings are returned as-is */
    expect(result.rows[0].metadata).toBe(
      '{"tags":["test","bigquery"],"settings":{"theme":"dark"}}',
    );
  });

  test('should parse registered JSON fields in SELECT results', async () => {
    const registeredConnection = new BigQueryConnection({
      options: { projectId: 'test-project' },
      jsonColumns: { 'dataset.users': ['metadata'] },
    });

    const mockRows = [{
      id: 1,
      name: 'John',
      metadata: '{"tags":["test","bigquery"],"settings":{"theme":"dark"}}',
    }];

    mockQuery.mockResolvedValue([mockRows]);

    const compiledQuery: CompiledQuery = {
      sql: 'SELECT * FROM users WHERE id = ?',
      parameters: [1],
      query: {} as any
    };

    const result = await registeredConnection.executeQuery<any>(compiledQuery);

    expect(result.rows[0].metadata).toEqual({
      tags: ['test', 'bigquery'],
      settings: { theme: 'dark' },
    });
  });

  test('should handle complex nested JSON objects', async () => {
    mockQuery.mockResolvedValue([[]]);

    const complexData = {
      user: {
        profile: {
          name: 'John Doe',
          age: 30,
          addresses: [
            { type: 'home', city: 'New York' },
            { type: 'work', city: 'San Francisco' }
          ]
        },
        settings: {
          notifications: {
            email: true,
            sms: false,
            push: {
              enabled: true,
              frequency: 'daily'
            }
          }
        }
      }
    };

    const compiledQuery: CompiledQuery = {
      sql: 'INSERT INTO user_data (id, data) VALUES (?, ?)',
      parameters: [1, complexData],
      query: {} as any
    };

    await connection.executeQuery(compiledQuery);

    /* BigQuery connection should pass objects as-is */
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO user_data (id, data) VALUES (?, ?)',
      params: [1, complexData],
      parseJSON: true,
    });
  });
});