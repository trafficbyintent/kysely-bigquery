import {Kysely, sql} from 'kysely';

import {BigQueryDialect} from '../src';
import {createBigQueryInstance} from './config';

export const expectedSimpleSelectCompiled = {
  parameters: [10, 1],
  query: {
    from: {
      froms: [
        {
          kind: 'TableNode',
          table: {
            identifier: {
              kind: 'IdentifierNode',
              name: 'metadata',
            },
            kind: 'SchemableIdentifierNode',
            schema: {
              kind: 'IdentifierNode',
              name: 'features',
            },
          },
        },
      ],
      kind: 'FromNode',
    },
    kind: 'SelectQueryNode',
    limit: {
      kind: 'LimitNode',
      limit: {
        kind: 'ValueNode',
        value: 1,
      },
    },
    selections: [
      {
        kind: 'SelectionNode',
        selection: {
          kind: 'SelectAllNode',
        },
      },
    ],
    where: {
      kind: 'WhereNode',
      where: {
        kind: 'BinaryOperationNode',
        leftOperand: {
          column: {
            column: {
              kind: 'IdentifierNode',
              name: 'id',
            },
            kind: 'ColumnNode',
          },
          kind: 'ReferenceNode',
          table: undefined,
        },
        operator: {
          kind: 'OperatorNode',
          operator: '>',
        },
        rightOperand: {
          kind: 'ValueNode',
          value: 10,
        },
      },
    },
  },
  sql: 'select * from `features`.`metadata` where `id` > ? limit ?',
};

// Test data fixtures
export const testUsers = [
  {
    id: '1',
    name: 'John Doe',
    email: 'john@example.com',
    created_at: new Date('2024-01-01'),
  },
  {
    id: '2',
    name: 'Jane Smith',
    email: 'jane@example.com',
    created_at: new Date('2024-01-02'),
  },
];

export const testProducts = [
  {
    id: '1',
    name: 'Laptop',
    price: 999.99,
    category: 'electronics',
    tags: ['computer', 'portable', 'work'],
  },
  {
    id: '2',
    name: 'Book',
    price: 19.99,
    category: 'books',
    tags: ['education', 'reading'],
  },
];

// Helper functions for integration tests
export async function createTestTable(
  kysely: Kysely<any>,
  dataset: string,
  tableName: string,
  schema: string,
): Promise<void> {
  const bigquery = createBigQueryInstance();
  const fullTableName = `${dataset}.${tableName}`;
  
  // Drop table if exists
  await kysely.schema.dropTable(fullTableName).ifExists().execute().catch(() => {});
  
  // Create table using raw SQL for BigQuery-specific syntax
  await sql.raw(schema).execute(kysely);
}

export async function cleanupTestTable(
  kysely: Kysely<any>,
  dataset: string,
  tableName: string,
): Promise<void> {
  const fullTableName = `${dataset}.${tableName}`;
  await kysely.schema.dropTable(fullTableName).ifExists().execute().catch(() => {});
}

// Test schemas
export const TEST_USERS_SCHEMA = `
CREATE TABLE IF NOT EXISTS test_dataset.test_users (
  id STRING NOT NULL,
  name STRING NOT NULL,
  email STRING,
  age INT64,
  tags ARRAY<STRING>,
  metadata STRUCT<
    source STRING,
    verified BOOL
  >,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
)`;

export const TEST_PRODUCTS_SCHEMA = `
CREATE TABLE IF NOT EXISTS test_dataset.test_products (
  id STRING NOT NULL,
  name STRING NOT NULL,
  price FLOAT64,
  category STRING,
  tags ARRAY<STRING>,
  details STRUCT<
    manufacturer STRING,
    weight_kg FLOAT64,
    dimensions STRUCT<
      length_cm FLOAT64,
      width_cm FLOAT64,
      height_cm FLOAT64
    >
  >,
  in_stock BOOL DEFAULT true,
  created_at TIMESTAMP NOT NULL
)`;

export const TEST_ORDERS_SCHEMA = `
CREATE TABLE IF NOT EXISTS test_dataset.test_orders (
  id STRING NOT NULL,
  customer_id STRING NOT NULL,
  product_id STRING NOT NULL,
  quantity INT64 NOT NULL,
  total_amount FLOAT64 NOT NULL,
  status STRING NOT NULL,
  order_date DATE NOT NULL,
  shipped_date DATE,
  metadata JSON,
  created_at TIMESTAMP NOT NULL
)`;

// Helper to generate test IDs
export function generateTestId(): string {
  return `test_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

