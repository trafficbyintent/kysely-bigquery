import { CompiledQuery } from 'kysely';
import { describe, expect, test, vi } from 'vitest';
import { BigQueryConnection } from '../src/bigQueryConnection';
import { BigQueryDialectConfig } from '../src';

describe('BigQuery JSON Query Builder Handling', () => {
  const mockQuery = vi.fn();
  const mockClient = {
    query: mockQuery,
    createQueryStream: vi.fn(),
  };

  const config: BigQueryDialectConfig = {
    bigquery: mockClient as any,
    jsonColumns: {
      'test_dataset.users': ['metadata', 'settings', 'preferences']
    }
  };

  const connection = new BigQueryConnection(config);

  test('should automatically stringify JSON fields in INSERT when column is JSON type', async () => {
    mockQuery.mockResolvedValue([[]]);

    const jsonData = {
      tags: ['test', 'bigquery'],
      settings: { theme: 'dark', notifications: true }
    };

    const compiledQuery: CompiledQuery = {
      sql: 'INSERT INTO users (id, name, metadata) VALUES (?, ?, ?)',
      parameters: [1, 'John', jsonData],
      query: {
        kind: 'InsertQueryNode',
        into: {
          kind: 'TableNode',
          table: {
            kind: 'SchemableIdentifierNode',
            schema: { kind: 'IdentifierNode', name: 'test_dataset' },
            identifier: { kind: 'IdentifierNode', name: 'users' }
          }
        },
        columns: [
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'id' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'name' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'metadata' } }
        ]
      } as any
    };

    await connection.executeQuery(compiledQuery);

    // Should stringify the JSON object for the metadata column
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO users (id, name, metadata) VALUES (?, ?, ?)',
      params: [1, 'John', JSON.stringify(jsonData)],
    });
  });

  test('should automatically stringify JSON fields in UPDATE when column is JSON type', async () => {
    mockQuery.mockResolvedValue([[]]);

    const newSettings = {
      preferences: { language: 'en', timezone: 'UTC' }
    };

    const compiledQuery: CompiledQuery = {
      sql: 'UPDATE users SET settings = ? WHERE id = ?',
      parameters: [newSettings, 1],
      query: {
        kind: 'UpdateQueryNode',
        table: {
          kind: 'TableNode',
          table: {
            kind: 'SchemableIdentifierNode',
            schema: { kind: 'IdentifierNode', name: 'test_dataset' },
            identifier: { kind: 'IdentifierNode', name: 'users' }
          }
        },
        updates: [
          {
            kind: 'ColumnUpdateNode',
            column: { 
              kind: 'ColumnNode', 
              column: { kind: 'IdentifierNode', name: 'settings' } 
            }
          }
        ]
      } as any
    };

    await connection.executeQuery(compiledQuery);

    // Should stringify the JSON object for the settings column
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'UPDATE users SET settings = ? WHERE id = ?',
      params: [JSON.stringify(newSettings), 1],
    });
  });

  test('should handle mixed JSON and non-JSON columns in INSERT', async () => {
    mockQuery.mockResolvedValue([[]]);

    const tags = ['admin', 'user'];
    const metadata = { role: 'admin', permissions: ['read', 'write'] };

    const compiledQuery: CompiledQuery = {
      sql: 'INSERT INTO users (id, name, tags, metadata) VALUES (?, ?, ?, ?)',
      parameters: [1, 'John', tags, metadata],
      query: {
        kind: 'InsertQueryNode',
        into: {
          kind: 'TableNode',
          table: {
            kind: 'SchemableIdentifierNode',
            schema: { kind: 'IdentifierNode', name: 'test_dataset' },
            identifier: { kind: 'IdentifierNode', name: 'users' }
          }
        },
        columns: [
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'id' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'name' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'tags' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'metadata' } }
        ]
      } as any
    };

    await connection.executeQuery(compiledQuery);

    // Should only stringify the metadata column (JSON), not tags (ARRAY)
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO users (id, name, tags, metadata) VALUES (?, ?, ?, ?)',
      params: [1, 'John', tags, JSON.stringify(metadata)],
    });
  });

  test('should not stringify arrays for ARRAY columns', async () => {
    mockQuery.mockResolvedValue([[]]);

    const tags = ['tag1', 'tag2', 'tag3'];

    const compiledQuery: CompiledQuery = {
      sql: 'INSERT INTO products (id, name, tags) VALUES (?, ?, ?)',
      parameters: [1, 'Product', tags],
      query: {
        kind: 'InsertQueryNode',
        into: {
          kind: 'TableNode',
          table: {
            kind: 'SchemableIdentifierNode',
            schema: { kind: 'IdentifierNode', name: 'test_dataset' },
            identifier: { kind: 'IdentifierNode', name: 'products' }
          }
        },
        columns: [
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'id' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'name' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'tags' } }
        ]
      } as any
    };

    await connection.executeQuery(compiledQuery);

    // Should NOT stringify arrays - BigQuery handles them natively
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO products (id, name, tags) VALUES (?, ?, ?)',
      params: [1, 'Product', tags],
    });
  });

  test('should handle null JSON values', async () => {
    mockQuery.mockResolvedValue([[]]);

    const compiledQuery: CompiledQuery = {
      sql: 'INSERT INTO users (id, name, metadata) VALUES (?, ?, ?)',
      parameters: [1, 'John', null],
      query: {
        kind: 'InsertQueryNode',
        into: {
          kind: 'TableNode',
          table: {
            kind: 'SchemableIdentifierNode',
            schema: { kind: 'IdentifierNode', name: 'test_dataset' },
            identifier: { kind: 'IdentifierNode', name: 'users' }
          }
        },
        columns: [
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'id' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'name' } },
          { kind: 'ColumnNode', column: { kind: 'IdentifierNode', name: 'metadata' } }
        ]
      } as any
    };

    await connection.executeQuery(compiledQuery);

    // Should handle null values with proper types
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'INSERT INTO users (id, name, metadata) VALUES (?, ?, ?)',
      params: [1, 'John', null],
      types: ['INT64', 'STRING', 'STRING']
    });
  });

  test('should handle UPDATE with multiple JSON columns', async () => {
    mockQuery.mockResolvedValue([[]]);

    const settings = { theme: 'light' };
    const preferences = { lang: 'en' };

    const compiledQuery: CompiledQuery = {
      sql: 'UPDATE users SET settings = ?, preferences = ? WHERE id = ?',
      parameters: [settings, preferences, 1],
      query: {
        kind: 'UpdateQueryNode',
        table: {
          kind: 'TableNode',
          table: {
            kind: 'SchemableIdentifierNode',
            schema: { kind: 'IdentifierNode', name: 'test_dataset' },
            identifier: { kind: 'IdentifierNode', name: 'users' }
          }
        },
        updates: [
          {
            kind: 'ColumnUpdateNode',
            column: { 
              kind: 'ColumnNode', 
              column: { kind: 'IdentifierNode', name: 'settings' } 
            }
          },
          {
            kind: 'ColumnUpdateNode',
            column: { 
              kind: 'ColumnNode', 
              column: { kind: 'IdentifierNode', name: 'preferences' } 
            }
          }
        ]
      } as any
    };

    await connection.executeQuery(compiledQuery);

    // Should stringify both JSON objects
    expect(mockQuery).toHaveBeenCalledWith({
      query: 'UPDATE users SET settings = ?, preferences = ? WHERE id = ?',
      params: [JSON.stringify(settings), JSON.stringify(preferences), 1],
    });
  });
});