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

  describe('isLikelyJsonColumn', () => {
    test('detects common JSON column names', () => {
      expect(detector.isLikelyJsonColumn('metadata')).toBe(true);
      expect(detector.isLikelyJsonColumn('settings')).toBe(true);
      expect(detector.isLikelyJsonColumn('config')).toBe(true);
      expect(detector.isLikelyJsonColumn('configuration')).toBe(true);
      expect(detector.isLikelyJsonColumn('preferences')).toBe(true);
      expect(detector.isLikelyJsonColumn('options')).toBe(true);
      expect(detector.isLikelyJsonColumn('data')).toBe(true);
      expect(detector.isLikelyJsonColumn('json')).toBe(true);
      expect(detector.isLikelyJsonColumn('payload')).toBe(true);
      expect(detector.isLikelyJsonColumn('body')).toBe(true);
      expect(detector.isLikelyJsonColumn('content')).toBe(true);
      expect(detector.isLikelyJsonColumn('attributes')).toBe(true);
      expect(detector.isLikelyJsonColumn('properties')).toBe(true);
      expect(detector.isLikelyJsonColumn('params')).toBe(true);
      expect(detector.isLikelyJsonColumn('extra')).toBe(true);
      expect(detector.isLikelyJsonColumn('custom')).toBe(true);
    });

    test('detects JSON columns case-insensitively', () => {
      expect(detector.isLikelyJsonColumn('METADATA')).toBe(true);
      expect(detector.isLikelyJsonColumn('MetaData')).toBe(true);
      expect(detector.isLikelyJsonColumn('SETTINGS')).toBe(true);
    });

    test('detects JSON columns with prefixes and suffixes', () => {
      expect(detector.isLikelyJsonColumn('user_metadata')).toBe(true);
      expect(detector.isLikelyJsonColumn('metadata_field')).toBe(true);
      expect(detector.isLikelyJsonColumn('product_json')).toBe(true);
      expect(detector.isLikelyJsonColumn('json_data')).toBe(true);
    });

    test('returns false for non-JSON column names', () => {
      expect(detector.isLikelyJsonColumn('id')).toBe(false);
      expect(detector.isLikelyJsonColumn('name')).toBe(false);
      expect(detector.isLikelyJsonColumn('email')).toBe(false);
      expect(detector.isLikelyJsonColumn('created_at')).toBe(false);
      expect(detector.isLikelyJsonColumn('price')).toBe(false);
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

    test('handles mismatched column and parameter counts', () => {
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (metadata) VALUES (?)',
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
            { column: { name: 'metadata' } },
          ],
        },
      };

      /* More params than columns */
      const params = [{ data: 'test' }, 'extra param'];
      const result = detector.processParameters(compiledQuery, params);
      
      /* Should not process when counts don't match */
      expect(result).toEqual([{ data: 'test' }, 'extra param']);
    });
  });

  describe('edge cases for extractTableName', () => {
    test('handles undefined tableNode', () => {
      /* Test via a query that would result in undefined tableNode */
      const compiledQuery: CompiledQuery = {
        sql: 'SELECT * FROM unknown_table',
        parameters: [],
        query: {
          kind: 'SelectQueryNode',
          from: {
            table: undefined, /* This should trigger the undefined handling */
          },
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      /* Should handle undefined gracefully and return undefined tableName */
      expect(info.tableName).toBeUndefined();
    });

    test('handles unrecognized table node types', () => {
      /* Test a table node with an unrecognized kind */
      const compiledQuery: CompiledQuery = {
        sql: 'SELECT * FROM weird_table',
        parameters: [],
        query: {
          kind: 'SelectQueryNode',
          from: {
            table: {
              kind: 'UnknownTableNodeType', /* This should trigger the default case */
              name: 'weird_table'
            },
          },
        },
      };

      const info = detector.extractTableAndColumns(compiledQuery);
      /* Should return undefined for unrecognized node types */
      expect(info.tableName).toBeUndefined();
    });

    test('handles null dataset id in table name parsing', () => {
      /* This tests handling when parts of the table reference are null/undefined */
      const compiledQuery: CompiledQuery = {
        sql: 'INSERT INTO dataset.users (metadata) VALUES (?)',
        parameters: [],
        query: {
          kind: 'InsertQueryNode',
          into: {
            table: {
              kind: 'SchemableIdentifierNode',
              schema: { name: null }, /* null schema name */
              identifier: { name: 'users' },
            },
          },
          columns: [
            { column: { name: 'metadata' } },
          ],
        },
      };

      const params = [{ role: 'admin' }];
      /* Should not crash when schema name is null */
      const result = detector.processParameters(compiledQuery, params);
      expect(result).toEqual([{ role: 'admin' }]); /* Should not process due to unclear table name */
    });

    test('directly tests extractTableName with undefined tableNode (edge case)', () => {
      /* Create a more direct test for the undefined case - lines 116-117 */
      const compiledQuery: CompiledQuery = {
        sql: 'SELECT * FROM somewhere',
        parameters: [],
        query: {
          kind: 'SelectQueryNode',
          from: {
            froms: [
              {
                table: undefined, /* This will hit the undefined check */
              }
            ]
          },
        },
      };

      /* This should trigger the undefined tableNode path without crashing */
      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBeUndefined();
    });

    test('directly tests extractTableName with completely unrecognized node (edge case)', () => {
      /* Test the final return undefined case - line 131 */
      const compiledQuery: CompiledQuery = {
        sql: 'SELECT * FROM weird_construct',
        parameters: [],
        query: {
          kind: 'SelectQueryNode',
          from: {
            table: {
              kind: 'CompletelyUnknownNodeType', /* This should hit line 131 */
              someProperty: 'value'
            } as any,
          },
        },
      };

      /* Should return undefined for completely unknown node types */
      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBeUndefined();
    });

    test('tests IdentifierNode with missing name property (defensive edge case)', () => {
      /* Test defensive programming for malformed IdentifierNode */
      const compiledQuery: CompiledQuery = {
        sql: 'SELECT * FROM malformed',
        parameters: [],
        query: {
          kind: 'SelectQueryNode',
          from: {
            table: {
              kind: 'IdentifierNode',
              /* Missing name property - should handle gracefully */
            } as any,
          },
        },
      };

      /* Should not crash even with malformed nodes */
      const info = detector.extractTableAndColumns(compiledQuery);
      expect(info.tableName).toBeUndefined();
    });
  });
});