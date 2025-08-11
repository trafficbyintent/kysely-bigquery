import { Kysely } from 'kysely';
import { describe, expect, test, vi, beforeEach } from 'vitest';

import { BigQueryDialect, BigQueryIntrospector } from '../src';

/* Mock BigQuery dataset and query responses */
const mockGetDatasets = vi.fn();
const mockQuery = vi.fn();

vi.mock('@google-cloud/bigquery', () => {
  return {
    BigQuery: class MockBigQuery {
      getDatasets = mockGetDatasets;
      query = mockQuery;
    },
  };
});

/* Mock Bluebird for concurrency testing */
vi.mock('bluebird', () => ({
  default: {
    map: vi.fn(async (items: any[], fn: any, options: any) => {
      /* Execute map function for each item sequentially */
      const results = [];
      for (const item of items) {
        results.push(await fn(item));
      }
      return results;
    }),
  },
}));

describe('BigQueryIntrospector', () => {
  let kysely: Kysely<any>;
  let introspector: BigQueryIntrospector;

  beforeEach(() => {
    vi.clearAllMocks();
    
    kysely = new Kysely<any>({
      dialect: new BigQueryDialect({ options: { projectId: 'test-project' } }),
    });
    
    introspector = new BigQueryIntrospector(kysely, { options: { projectId: 'test-project' } });
  });

  describe('getSchemas', () => {
    test('returns schema metadata from datasets', async () => {
      const mockDatasets = [
        { id: 'dataset1' },
        { id: 'dataset2' },
        { id: 'dataset3' },
      ];

      mockGetDatasets.mockResolvedValue([mockDatasets]);

      const schemas = await introspector.getSchemas();

      expect(schemas).toHaveLength(3);
      expect(schemas[0]).toEqual({ name: 'dataset1' });
      expect(schemas[1]).toEqual({ name: 'dataset2' });
      expect(schemas[2]).toEqual({ name: 'dataset3' });
      expect(Object.isFrozen(schemas[0])).toBe(true);
    });

    test('handles empty dataset list', async () => {
      mockGetDatasets.mockResolvedValue([[]]);

      const schemas = await introspector.getSchemas();

      expect(schemas).toEqual([]);
    });

    test('handles datasets with null/undefined ids', async () => {
      const mockDatasets = [
        { id: 'dataset1' },
        { id: null },
        { id: undefined },
        { id: 'dataset2' },
      ];

      mockGetDatasets.mockResolvedValue([mockDatasets]);

      const schemas = await introspector.getSchemas();

      expect(schemas).toHaveLength(4);
      expect(schemas[0].name).toBe('dataset1');
      expect(schemas[1].name).toBe('');
      expect(schemas[2].name).toBe('');
      expect(schemas[3].name).toBe('dataset2');
    });
  });

  describe('getTables', () => {
    test('returns table metadata from INFORMATION_SCHEMA', async () => {
      const mockDatasets = [
        { id: 'dataset1' },
        { id: 'dataset2' },
      ];

      const mockColumns1 = [
        {
          table_schema: 'dataset1',
          table_name: 'users',
          column_name: 'id',
          is_nullable: 'NO',
          data_type: 'INTEGER',
          column_default: null,
        },
        {
          table_schema: 'dataset1',
          table_name: 'users',
          column_name: 'name',
          is_nullable: 'YES',
          data_type: 'STRING',
          column_default: "'anonymous'",
        },
      ];

      const mockColumns2 = [
        {
          table_schema: 'dataset2',
          table_name: 'products',
          column_name: 'id',
          is_nullable: 'NO',
          data_type: 'INTEGER',
          column_default: null,
        },
      ];

      mockGetDatasets.mockResolvedValue([mockDatasets]);
      
      /* Mock db.selectFrom().selectAll().execute() chain */
      const mockExecute = vi.fn()
        .mockResolvedValueOnce(mockColumns1)
        .mockResolvedValueOnce(mockColumns2);
      
      const mockSelectAll = vi.fn().mockReturnValue({ execute: mockExecute });
      const mockSelectFrom = vi.fn().mockReturnValue({ selectAll: mockSelectAll });
      
      vi.spyOn(kysely, 'selectFrom').mockImplementation(mockSelectFrom as any);

      const tables = await introspector.getTables();

      expect(tables).toHaveLength(2);
      
      /* First table */
      expect(tables[0].name).toBe('users');
      expect(tables[0].schema).toBe('dataset1');
      expect(tables[0].isView).toBe(false);
      expect(tables[0].columns).toHaveLength(2);
      
      /* First column */
      expect(tables[0].columns[0]).toEqual({
        name: 'id',
        dataType: 'INTEGER',
        hasDefaultValue: false,
        isAutoIncrementing: false,
        isNullable: false,
      });
      
      /* Second column with default value */
      expect(tables[0].columns[1]).toEqual({
        name: 'name',
        dataType: 'STRING',
        hasDefaultValue: true,
        isAutoIncrementing: false,
        isNullable: true,
      });
      
      /* Second table */
      expect(tables[1].name).toBe('products');
      expect(tables[1].schema).toBe('dataset2');
    });

    test('handles empty column results', async () => {
      const mockDatasets = [{ id: 'empty_dataset' }];

      mockGetDatasets.mockResolvedValue([mockDatasets]);
      
      const mockExecute = vi.fn().mockResolvedValueOnce([]);
      const mockSelectAll = vi.fn().mockReturnValue({ execute: mockExecute });
      const mockSelectFrom = vi.fn().mockReturnValue({ selectAll: mockSelectAll });
      
      vi.spyOn(kysely, 'selectFrom').mockImplementation(mockSelectFrom as any);

      const tables = await introspector.getTables();

      expect(tables).toEqual([]);
    });

    test('handles NULL column_default correctly', async () => {
      const mockDatasets = [{ id: 'dataset1' }];

      const mockColumns = [
        {
          table_schema: 'dataset1',
          table_name: 'test_table',
          column_name: 'col1',
          is_nullable: 'YES',
          data_type: 'STRING',
          column_default: 'NULL', /* String 'NULL' should be treated as no default */
        },
        {
          table_schema: 'dataset1',
          table_name: 'test_table',
          column_name: 'col2',
          is_nullable: 'NO',
          data_type: 'INTEGER',
          column_default: '0', /* Actual default value */
        },
      ];

      mockGetDatasets.mockResolvedValue([mockDatasets]);
      
      const mockExecute = vi.fn().mockResolvedValueOnce(mockColumns);
      const mockSelectAll = vi.fn().mockReturnValue({ execute: mockExecute });
      const mockSelectFrom = vi.fn().mockReturnValue({ selectAll: mockSelectAll });
      
      vi.spyOn(kysely, 'selectFrom').mockImplementation(mockSelectFrom as any);

      const tables = await introspector.getTables();

      expect(tables[0].columns[0].hasDefaultValue).toBe(false);
      expect(tables[0].columns[1].hasDefaultValue).toBe(true);
    });

    test('groups columns by table correctly', async () => {
      const mockDatasets = [{ id: 'dataset1' }];

      const mockColumns = [
        {
          table_schema: 'dataset1',
          table_name: 'users',
          column_name: 'id',
          is_nullable: 'NO',
          data_type: 'INTEGER',
          column_default: null,
        },
        {
          table_schema: 'dataset1',
          table_name: 'posts',
          column_name: 'id',
          is_nullable: 'NO',
          data_type: 'INTEGER',
          column_default: null,
        },
        {
          table_schema: 'dataset1',
          table_name: 'users',
          column_name: 'email',
          is_nullable: 'YES',
          data_type: 'STRING',
          column_default: null,
        },
      ];

      mockGetDatasets.mockResolvedValue([mockDatasets]);
      
      const mockExecute = vi.fn().mockResolvedValueOnce(mockColumns);
      const mockSelectAll = vi.fn().mockReturnValue({ execute: mockExecute });
      const mockSelectFrom = vi.fn().mockReturnValue({ selectAll: mockSelectAll });
      
      vi.spyOn(kysely, 'selectFrom').mockImplementation(mockSelectFrom as any);

      const tables = await introspector.getTables();

      expect(tables).toHaveLength(2);
      
      const usersTable = tables.find(t => t.name === 'users');
      const postsTable = tables.find(t => t.name === 'posts');
      
      expect(usersTable?.columns).toHaveLength(2);
      expect(postsTable?.columns).toHaveLength(1);
    });
  });

  describe('getMetadata', () => {
    test('returns database metadata with tables', async () => {
      const mockDatasets = [{ id: 'dataset1' }];
      const mockColumns = [
        {
          table_schema: 'dataset1',
          table_name: 'users',
          column_name: 'id',
          is_nullable: 'NO',
          data_type: 'INTEGER',
          column_default: null,
        },
      ];

      mockGetDatasets.mockResolvedValue([mockDatasets]);
      
      const mockExecute = vi.fn().mockResolvedValueOnce(mockColumns);
      const mockSelectAll = vi.fn().mockReturnValue({ execute: mockExecute });
      const mockSelectFrom = vi.fn().mockReturnValue({ selectAll: mockSelectAll });
      
      vi.spyOn(kysely, 'selectFrom').mockImplementation(mockSelectFrom as any);

      const metadata = await introspector.getMetadata();

      expect(metadata).toHaveProperty('tables');
      expect(metadata.tables).toHaveLength(1);
      expect(metadata.tables[0].name).toBe('users');
    });

    test('passes options to getTables', async () => {
      const mockDatasets = [{ id: 'dataset1' }];
      mockGetDatasets.mockResolvedValue([mockDatasets]);
      
      const mockExecute = vi.fn().mockResolvedValueOnce([]);
      const mockSelectAll = vi.fn().mockReturnValue({ execute: mockExecute });
      const mockSelectFrom = vi.fn().mockReturnValue({ selectAll: mockSelectAll });
      
      vi.spyOn(kysely, 'selectFrom').mockImplementation(mockSelectFrom as any);

      /* Spy on getTables to verify options are passed */
      const getTablesSpy = vi.spyOn(introspector, 'getTables');

      const options = { withInternalKyselyTables: true };
      await introspector.getMetadata(options);

      expect(getTablesSpy).toHaveBeenCalledWith(options);
    });

    test('handles null/undefined dataset id in dataset list', async () => {
      /* Mock datasets with null/undefined ids to test the nullish coalescing operator */
      const mockDatasetsWithNullIds = [
        { id: 'valid_dataset' },
        { id: null },
        { id: undefined },
        { /* no id property */ },
      ];

      mockGetDatasets.mockResolvedValue([mockDatasetsWithNullIds]);
      
      /* Mock empty results for each dataset query attempt */
      const mockExecute = vi.fn().mockResolvedValue([]);
      const mockSelectAll = vi.fn().mockReturnValue({ execute: mockExecute });
      const mockSelectFrom = vi.fn().mockReturnValue({ selectAll: mockSelectAll });
      
      vi.spyOn(kysely, 'selectFrom').mockImplementation(mockSelectFrom as any);

      const tables = await introspector.getTables();
      
      /* Should attempt to query each dataset - this tests the nullish coalescing operator */
      /* The critical test is that it doesn't crash when id is null/undefined */
      expect(mockSelectFrom).toHaveBeenCalledTimes(4); /* Once for each dataset */
      
      /* The important thing is that all calls were made without throwing */
      /* This ensures the nullish coalescing operator (id ?? '') works correctly */
      expect(tables).toEqual([]);
    });
  });
});