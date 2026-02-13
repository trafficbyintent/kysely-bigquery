import { describe, expect, test, beforeEach } from 'vitest';
import { CompiledQuery } from 'kysely';

import { JsonColumnDetector } from '../src/jsonColumnDetector';

describe('JsonColumnDetector', () => {
  let detector: JsonColumnDetector;

  beforeEach(() => {
    detector = new JsonColumnDetector();
    /* Register JSON columns for testing */
    detector.registerJsonColumns('dataset.users', ['metadata', 'settings']);
    detector.registerJsonColumns('dataset.products', ['specifications']);
  });

  describe('isJsonColumn', () => {
    test('detects registered JSON columns', () => {
      expect(detector.isJsonColumn('dataset.users', 'metadata')).toBe(true);
      expect(detector.isJsonColumn('dataset.users', 'settings')).toBe(true);
      expect(detector.isJsonColumn('dataset.products', 'specifications')).toBe(true);
    });

    test('returns false for non-JSON columns', () => {
      expect(detector.isJsonColumn('dataset.users', 'id')).toBe(false);
      expect(detector.isJsonColumn('dataset.users', 'name')).toBe(false);
      expect(detector.isJsonColumn('unknown.table', 'metadata')).toBe(false);
    });
  });

  describe('extractTableAndColumns', () => {
    test('extracts table name from insert query with SchemableIdentifierNode', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (id, name) VALUES (?, ?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: 'dataset' },
              identifier: { name: 'users' },
            },
          },
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBe('dataset.users');
    });

    test('extracts table name from simple IdentifierNode', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO users (id, name) VALUES (?, ?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'IdentifierNode',
              name: 'users',
            },
          },
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBe('users');
    });

    test('handles missing query or table information', () => {
      const compiledQuery1: CompiledQuery = {
        sql: 'SELECT 1',
        parameters: [],
        query: undefined,
      };

      const info1 = detector.extractTableAndColumns(compiledQuery1);
      expect(info1).toEqual({});

      const compiledQuery2: CompiledQuery = {
        sql: 'INSERT INTO users',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: undefined,
        },
      };

      const info2 = detector.extractTableAndColumns(compiledQuery2);
      expect(info2).toEqual({});
    });

    test('extracts column names from insert query', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO users',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: { kind: 'IdentifierNode', name: 'users' },
          },
          columns: [
            { column: { name: 'id' } },
            { column: { name: 'metadata' } },
            { column: { name: 'settings' } },
          ],
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.columns).toEqual(['id', 'metadata', 'settings']);
    });

    test('extracts update columns from update query', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'UPDATE users SET metadata = ?, status = ?',
        parameters: [],
        query: {
          kind: 'UpdateQueryNode',
          table: {
            table: { kind: 'IdentifierNode', name: 'users' },
          },
          updates: [
            { 
              column: { 
                column: { name: 'metadata' } 
              },
            },
            { 
              column: { 
                column: { name: 'status' } 
              },
            },
          ],
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.updateColumns).toEqual(['metadata', 'status']);
    });

    test('handles columns without proper structure', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO users',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: { kind: 'IdentifierNode', name: 'users' },
          },
          columns: [
            { column: { name: 'id' } },
            { column: undefined }, /* Missing column */
            { name: 'direct_name' }, /* Direct name property */
          ],
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.columns).toEqual(['id', 'direct_name']); /* Valid column names */
    });

    test('returns only defined properties in result', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'UPDATE dataset.users SET metadata = ?',
        parameters: [],
        query: {
          kind: 'UpdateQueryNode',
          table: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: 'dataset' },
              identifier: { name: 'users' },
            },
          },
          updates: [
            { 
              column: { 
                column: { name: 'metadata' } 
              },
            },
          ],
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info).toEqual({
        tableName: 'dataset.users',
        updateColumns: ['metadata'],
      });
      /* columns property should not be included when undefined */
      expect(info).not.toHaveProperty('columns');
    });

    test('handles table without schema in SchemableIdentifierNode', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO users',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: undefined,
              identifier: { name: 'users' },
            },
          },
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBe('users');
    });

    test('handles update query with direct column names', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'UPDATE users SET col = ?',
        parameters: [],
        query: {
          kind: 'UpdateQueryNode',
          table: {
            table: { kind: 'IdentifierNode', name: 'users' },
          },
          updates: [
            { 
              column: { name: 'direct_column' }, /* Direct name property */
            },
          ],
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.updateColumns).toEqual(['direct_column']);
    });
  });

  describe('processParameters', () => {
    test('serializes objects for registered JSON columns in INSERT', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (id, metadata) VALUES (?, ?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: 'dataset' },
              identifier: { name: 'users' },
            },
          },
          columns: [
            { column: { name: 'id' } },
            { column: { name: 'metadata' } },
          ],
        },
      };

      const params = [1, { role: 'admin', permissions: ['read', 'write'] }];
      const result = detector.processParameters(compiledQuery, params);
      
      expect(result[0]).toBe(1);
      expect(result[1]).toBe('{"role":"admin","permissions":["read","write"]}');
    });

    test('serializes objects for registered JSON columns in UPDATE', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'UPDATE dataset.users SET metadata = ? WHERE id = ?',
        parameters: [],
        query: {
          kind: 'UpdateQueryNode',
          table: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: 'dataset' },
              identifier: { name: 'users' },
            },
          },
          updates: [
            { 
              column: { 
                column: { name: 'metadata' } 
              },
            },
          ],
        },
      };

      const params = [{ role: 'admin' }, 1];
      const result = detector.processParameters(compiledQuery, params);
      
      expect(result[0]).toBe('{"role":"admin"}');
      expect(result[1]).toBe(1); /* WHERE parameter unchanged */
    });

    test('does not serialize non-object values', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (id, metadata, settings) VALUES (?, ?, ?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: 'dataset' },
              identifier: { name: 'users' },
            },
          },
          columns: [
            { column: { name: 'id' } },
            { column: { name: 'metadata' } },
            { column: { name: 'settings' } },
          ],
        },
      };

      const params = [1, 'string value', null];
      const result = detector.processParameters(compiledQuery, params);
      
      expect(result).toEqual([1, 'string value', null]);
    });

    test('does not serialize Date or Buffer objects', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (id, metadata) VALUES (?, ?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: 'dataset' },
              identifier: { name: 'users' },
            },
          },
          columns: [
            { column: { name: 'id' } },
            { column: { name: 'metadata' } },
          ],
        },
      };

      const date = new Date();
      const buffer = Buffer.from('test');
      const params = [date, buffer];
      const result = detector.processParameters(compiledQuery, params);
      
      expect(result[0]).toBe(date);
      expect(result[1]).toBe(buffer);
    });

    test('returns original params when table name cannot be extracted', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'SELECT * FROM users',
        parameters: [],
        query: {
          kind: 'SelectQueryNode',
        },
      };

      const params = [1, 2, 3];
      const result = detector.processParameters(compiledQuery, params);
      
      expect(result).toEqual([1, 2, 3]);
    });

    test('serializes JSON columns in multi-row INSERT', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (id, metadata) VALUES (?, ?), (?, ?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: 'dataset' },
              identifier: { name: 'users' },
            },
          },
          columns: [
            { column: { name: 'id' } },
            { column: { name: 'metadata' } },
          ],
        },
      };

      const meta1 = { role: 'admin' };
      const meta2 = { role: 'user' };
      const params = [1, meta1, 2, meta2];
      const result = detector.processParameters(compiledQuery, params);

      /* Should stringify metadata in both rows */
      expect(result[0]).toBe(1);
      expect(result[1]).toBe(JSON.stringify(meta1));
      expect(result[2]).toBe(2);
      expect(result[3]).toBe(JSON.stringify(meta2));
    });

    test('skips serialization when params length is not a multiple of columns', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (id, metadata) VALUES (?, ?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: 'dataset' },
              identifier: { name: 'users' },
            },
          },
          columns: [
            { column: { name: 'id' } },
            { column: { name: 'metadata' } },
          ],
        },
      };

      /* 3 params for 2 columns — not a valid multiple, skip processing */
      const params = [1, { data: 'test' }, 'orphan'];
      const result = detector.processParameters(compiledQuery, params);

      expect(result).toEqual([1, { data: 'test' }, 'orphan']);
    });
  });

  describe('edge cases for extractTableName', () => {
    test('handles undefined into.table in InsertQueryNode', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO unknown (col) VALUES (?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: undefined,
          },
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBeUndefined();
    });

    test('handles unrecognized table node type in InsertQueryNode', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO weird (col) VALUES (?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'UnknownTableNodeType',
              name: 'weird_table',
            } as any,
          },
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBeUndefined();
    });

    test('handles null schema name in InsertQueryNode', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (metadata) VALUES (?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: null },
              identifier: { name: 'users' },
            },
          },
          columns: [{ column: { name: 'metadata' } }],
        },
      };

      const params = [{ role: 'admin' }];
      /* Should not crash when schema name is null */
      const result = detector.processParameters(compiledQuery, params);
      expect(result).toEqual([{ role: 'admin' }]);
    });

    test('handles undefined table.table in UpdateQueryNode', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'UPDATE unknown SET col = ?',
        parameters: [],
        query: {
          kind: 'UpdateQueryNode',
          table: {
            table: undefined,
          },
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBeUndefined();
    });

    test('handles unrecognized node type in UpdateQueryNode', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'UPDATE weird SET col = ?',
        parameters: [],
        query: {
          kind: 'UpdateQueryNode',
          table: {
            table: {
              kind: 'CompletelyUnknownNodeType',
              someProperty: 'value',
            } as any,
          },
          updates: [{ column: { column: { name: 'col' } } }],
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBeUndefined();
    });
  });
});