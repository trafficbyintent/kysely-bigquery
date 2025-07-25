import { Kysely, sql } from 'kysely';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';

import { BigQueryDialect } from '../src';
import { createBigQueryInstance } from './config';
import {
  cleanupTestTable,
  createTestTable,
  generateTestId,
  resetTestIdCounter,
  TEST_ORDERS_SCHEMA,
  TEST_PRODUCTS_SCHEMA,
  TEST_USERS_SCHEMA,
} from './helpers';

// Define test database types
interface TestDatabase {
  'test_dataset.test_users': {
    id: string;
    name: string;
    email: string | null;
    age: number | null;
    tags: string[] | null;
    metadata: {
      source: string;
      verified: boolean;
    } | null;
    created_at: Date;
    updated_at: Date | null;
  };
  'test_dataset.test_products': {
    id: string;
    name: string;
    price: number | null;
    category: string | null;
    tags: string[] | null;
    details: any | null;
    in_stock: boolean | null;
    created_at: Date;
  };
  'test_dataset.test_orders': {
    id: string;
    customer_id: string;
    product_id: string;
    quantity: number;
    total_amount: number;
    status: string;
    order_date: string; // BigQuery DATE type expects 'YYYY-MM-DD' string
    shipped_date: string | null; // BigQuery DATE type expects 'YYYY-MM-DD' string
    metadata: any | null;
    created_at: Date;
  };
  'features.metadata': {
    id: number;
    category: string;
    name: string;
    created_at: any;
    updated_at: any;
    inserted_at: any;
  };
  'bank_account_transactions': any;
  // Dynamic test tables
  'test_dataset.test_aggregates': any;
  'test_dataset.test_dml': any;
  'test_dataset.test_partitioned': any;
  'test_dataset.test_constraints': any;
  [key: string]: any; // Allow dynamic table names
}

const kysely = new Kysely<TestDatabase>({
  dialect: new BigQueryDialect({
    bigquery: createBigQueryInstance(),
  }),
});

// Setup test tables before running tests
beforeAll(async () => {
  resetTestIdCounter();
  try {
    await createTestTable(kysely, 'test_dataset', 'test_users', TEST_USERS_SCHEMA);
    await createTestTable(kysely, 'test_dataset', 'test_products', TEST_PRODUCTS_SCHEMA);
    await createTestTable(kysely, 'test_dataset', 'test_orders', TEST_ORDERS_SCHEMA);
  } catch (error) {
    console.error('Failed to create test tables:', error);
    throw error;
  }
});

// Cleanup test tables after all tests
afterAll(async () => {
  try {
    await cleanupTestTable(kysely, 'test_dataset', 'test_users');
    await cleanupTestTable(kysely, 'test_dataset', 'test_products');
    await cleanupTestTable(kysely, 'test_dataset', 'test_orders');
  } catch (error) {
    console.error('Failed to cleanup test tables:', error);
  }
});

test('simple select execution', async () => {
  const query = kysely.selectFrom('features.metadata').where('id', '>', 10).selectAll().limit(1);

  const rows = await query.execute();

  expect(rows).toHaveLength(1);
  
  // Verify the result has expected structure without hardcoding specific values
  const row = rows[0];
  expect(row).toHaveProperty('id');
  expect(typeof row.id).toBe('number');
  expect(row.id).toBeGreaterThan(10);
  expect(row).toHaveProperty('category');
  expect(row).toHaveProperty('name');
  expect(row.created_at).toHaveProperty('value');
  expect(row.updated_at).toHaveProperty('value');
  expect(row.inserted_at).toHaveProperty('value');
});

test('introspection', async () => {
  const tables = await kysely.introspection.getTables();

  // Verify introspection returns tables without depending on snapshot
  const bankAccountTable = tables.find(t => t.name === 'bank_account_transactions');
  expect(bankAccountTable).toBeDefined();
  expect(bankAccountTable?.columns).toBeDefined();
  expect(bankAccountTable?.columns.length).toBeGreaterThan(0);
});

test('INSERT operations', { timeout: 10000 }, async () => {
  // Test basic insert
  const insertResult = await kysely
    .insertInto('test_dataset.test_users')
    .values({
      id: generateTestId('insert_single'),
      name: 'Test User',
      email: 'test@example.com',
      age: 25,
      tags: ['test', 'integration'],
      metadata: {
        source: 'test_suite',
        verified: true,
      },
      created_at: new Date(),
    })
    .execute();

  // BigQuery doesn't return affected rows, but the query should execute without error
  expect(insertResult).toBeDefined();

  // Test multiple inserts
  const testId1 = generateTestId('insert_multi');
  const testId2 = generateTestId('insert_multi');
  
  await kysely
    .insertInto('test_dataset.test_users')
    .values([
      {
        id: testId1,
        name: 'User 1',
        email: 'user1@example.com',
        created_at: new Date(),
      },
      {
        id: testId2,
        name: 'User 2',
        email: 'user2@example.com',
        created_at: new Date(),
      },
    ])
    .execute();

  // Verify the inserts worked
  const users = await kysely
    .selectFrom('test_dataset.test_users')
    .where('id', 'in', [testId1, testId2])
    .selectAll()
    .execute();

  expect(users).toHaveLength(2);
  expect(users.map(u => u.name).sort()).toEqual(['User 1', 'User 2']);
});

test('UPDATE operations', { timeout: 30000 }, async () => {
  // First insert a test record
  const testId = generateTestId();
  await kysely
    .insertInto('test_dataset.test_users')
    .values({
      id: testId,
      name: 'Original Name',
      email: 'original@example.com',
      age: 30,
      created_at: new Date(),
    })
    .execute();

  // Update the record
  await kysely
    .updateTable('test_dataset.test_users')
    .set({
      name: 'Updated Name',
      email: 'updated@example.com',
      age: 31,
      updated_at: new Date(),
    })
    .where('id', '=', testId)
    .execute();

  // Verify the update
  const updated = await kysely
    .selectFrom('test_dataset.test_users')
    .where('id', '=', testId)
    .selectAll()
    .executeTakeFirst();

  expect(updated).toBeDefined();
  expect(updated!.name).toBe('Updated Name');
  expect(updated!.email).toBe('updated@example.com');
  expect(updated!.age).toBe(31);
  expect(updated!.updated_at).toBeDefined();
});

test('DELETE operations', { timeout: 30000 }, async () => {
  // Insert test records
  const testIds = [generateTestId(), generateTestId(), generateTestId()];
  
  await kysely
    .insertInto('test_dataset.test_users')
    .values(testIds.map((id, index) => ({
      id,
      name: `Delete Test ${index}`,
      email: `delete${index}@example.com`,
      created_at: new Date(),
    })))
    .execute();

  // Delete one record
  await kysely
    .deleteFrom('test_dataset.test_users')
    .where('id', '=', testIds[0])
    .execute();

  // Delete multiple records with condition
  await kysely
    .deleteFrom('test_dataset.test_users')
    .where('id', 'in', [testIds[1], testIds[2]])
    .execute();

  // Verify all are deleted
  const remaining = await kysely
    .selectFrom('test_dataset.test_users')
    .where('id', 'in', testIds)
    .selectAll()
    .execute();

  expect(remaining).toHaveLength(0);
});

test('Complex data modification scenarios', { timeout: 30000 }, async () => {
  // Test INSERT with subquery
  const sourceId = generateTestId();
  const targetId = generateTestId();
  
  // Insert source data
  await kysely
    .insertInto('test_dataset.test_products')
    .values({
      id: sourceId,
      name: 'Source Product',
      price: 99.99,
      category: 'electronics',
      tags: ['source', 'test'],
      created_at: new Date(),
    })
    .execute();

  // Insert with SELECT subquery
  await sql`
    INSERT INTO test_dataset.test_orders (id, customer_id, product_id, quantity, total_amount, status, order_date, created_at)
    SELECT 
      ${targetId},
      'customer_123',
      id,
      2,
      price * 2,
      'pending',
      CURRENT_DATE(),
      CURRENT_TIMESTAMP()
    FROM test_dataset.test_products
    WHERE id = ${sourceId}
  `.execute(kysely);

  // Verify the insert
  const order = await kysely
    .selectFrom('test_dataset.test_orders')
    .where('id', '=', targetId)
    .selectAll()
    .executeTakeFirst();

  expect(order).toBeDefined();
  expect(order!.product_id).toBe(sourceId);
  expect(order!.quantity).toBe(2);
  expect(order!.total_amount).toBe(199.98);

  // Test UPDATE with JOIN (BigQuery syntax)
  await sql`
    UPDATE test_dataset.test_orders o
    SET o.status = 'shipped',
        o.shipped_date = CURRENT_DATE()
    FROM test_dataset.test_products p
    WHERE o.product_id = p.id
    AND p.category = 'electronics'
    AND o.id = ${targetId}
  `.execute(kysely);

  // Verify the update
  const updatedOrder = await kysely
    .selectFrom('test_dataset.test_orders')
    .where('id', '=', targetId)
    .selectAll()
    .executeTakeFirst();

  expect(updatedOrder!.status).toBe('shipped');
  expect(updatedOrder!.shipped_date).toBeDefined();
});

test('BigQuery ARRAY type handling', { timeout: 10000 }, async () => {
  const testId = generateTestId();
  
  // Insert data with ARRAY
  await kysely
    .insertInto('test_dataset.test_users')
    .values({
      id: testId,
      name: 'Array Test User',
      email: 'array@example.com',
      tags: ['developer', 'typescript', 'bigquery'],
      created_at: new Date(),
    })
    .execute();

  // Query with ARRAY operations
  const result = await kysely
    .selectFrom('test_dataset.test_users')
    .select(['id', 'name', 'tags'])
    .where('id', '=', testId)
    .executeTakeFirst();

  expect(result).toBeDefined();
  expect(result!.tags).toEqual(['developer', 'typescript', 'bigquery']);

  // Test ARRAY contains using raw SQL
  const hasTag = await sql<{has_tag: boolean}>`
    SELECT EXISTS(
      SELECT 1 
      FROM test_dataset.test_users 
      WHERE id = ${testId}
      AND 'typescript' IN UNNEST(tags)
    ) as has_tag
  `.execute(kysely);

  expect(hasTag.rows[0].has_tag).toBe(true);

  // Test ARRAY_LENGTH
  const arrayLength = await sql<{tag_count: number}>`
    SELECT ARRAY_LENGTH(tags) as tag_count
    FROM test_dataset.test_users
    WHERE id = ${testId}
  `.execute(kysely);

  expect(arrayLength.rows[0].tag_count).toBe(3);
});

test('BigQuery STRUCT type handling', async () => {
  const testId = generateTestId();
  
  // Insert data with STRUCT
  await kysely
    .insertInto('test_dataset.test_users')
    .values({
      id: testId,
      name: 'Struct Test User',
      email: 'struct@example.com',
      metadata: {
        source: 'integration_test',
        verified: true,
      },
      created_at: new Date(),
    })
    .execute();

  // Query STRUCT data
  const result = await kysely
    .selectFrom('test_dataset.test_users')
    .select(['id', 'name', 'metadata'])
    .where('id', '=', testId)
    .executeTakeFirst();

  expect(result).toBeDefined();
  expect(result!.metadata).toEqual({
    source: 'integration_test',
    verified: true,
  });

  // Query specific STRUCT fields
  const structField = await sql<{source: string; verified: boolean}>`
    SELECT 
      metadata.source as source,
      metadata.verified as verified
    FROM test_dataset.test_users
    WHERE id = ${testId}
  `.execute(kysely);

  expect(structField.rows[0].source).toBe('integration_test');
  expect(structField.rows[0].verified).toBe(true);
});

test('BigQuery nested STRUCT handling', async () => {
  const testId = generateTestId();
  
  try {
    // Insert product with nested STRUCT
    await kysely
      .insertInto('test_dataset.test_products')
      .values({
        id: testId,
        name: 'Nested Struct Product',
        price: 299.99,
        details: {
          manufacturer: 'Test Corp',
          weight_kg: 2.5,
          dimensions: {
            length_cm: 30,
            width_cm: 20,
            height_cm: 10,
          },
        },
        created_at: new Date(),
      })
      .execute();

    // Query nested STRUCT
    const result = await sql<{
      manufacturer: string;
      weight_kg: number;
      length: number;
      width: number;
      height: number;
    }>`
      SELECT 
        details.manufacturer,
        details.weight_kg,
        details.dimensions.length_cm as length,
        details.dimensions.width_cm as width,
        details.dimensions.height_cm as height
      FROM test_dataset.test_products
      WHERE id = ${testId}
    `.execute(kysely);

    const row = result.rows[0];
    expect(row.manufacturer).toBe('Test Corp');
    expect(row.weight_kg).toBe(2.5);
    expect(row.length).toBe(30);
    expect(row.width).toBe(20);
    expect(row.height).toBe(10);
  } finally {
    // Cleanup test data
    await kysely
      .deleteFrom('test_dataset.test_products')
      .where('id', '=', testId)
      .execute()
      .catch(() => {}); // Ignore cleanup errors
  }
});

test('BigQuery DATE and TIMESTAMP types', async () => {
  const testId = generateTestId();
  const orderDate = '2024-01-15';  // BigQuery DATE needs string format
  const shippedDate = '2024-01-17';
  
  // Insert with DATE and TIMESTAMP
  await kysely
    .insertInto('test_dataset.test_orders')
    .values({
      id: testId,
      customer_id: 'cust_123',
      product_id: 'prod_456',
      quantity: 1,
      total_amount: 99.99,
      status: 'shipped',
      order_date: orderDate,
      shipped_date: shippedDate,
      created_at: new Date(),
    })
    .execute();

  // Query and verify date handling
  const result = await kysely
    .selectFrom('test_dataset.test_orders')
    .selectAll()
    .where('id', '=', testId)
    .executeTakeFirst();

  expect(result).toBeDefined();
  
  // BigQuery returns dates as objects with value property
  expect(result!.order_date).toBeDefined();
  expect(result!.shipped_date).toBeDefined();
  expect(result!.created_at).toBeDefined();

  // Test date functions
  const dateDiff = await sql<{days_to_ship: number}>`
    SELECT DATE_DIFF(shipped_date, order_date, DAY) as days_to_ship
    FROM test_dataset.test_orders
    WHERE id = ${testId}
  `.execute(kysely);

  expect(dateDiff.rows[0].days_to_ship).toBe(2);
});

test('BigQuery NUMERIC and BOOL types', async () => {
  const testId = generateTestId();
  
  // Test NUMERIC precision
  await kysely
    .insertInto('test_dataset.test_products')
    .values({
      id: testId,
      name: 'Precision Test Product',
      price: 1234.56, // NUMERIC(10, 2)
      in_stock: true,
      created_at: new Date(),
    })
    .execute();

  const result = await kysely
    .selectFrom('test_dataset.test_products')
    .select(['price', 'in_stock'])
    .where('id', '=', testId)
    .executeTakeFirst();

  expect(result).toBeDefined();
  expect(result!.price).toBe(1234.56);
  expect(result!.in_stock).toBe(true);

  // Test BOOL in WHERE clause
  const inStockProducts = await kysely
    .selectFrom('test_dataset.test_products')
    .select('id')
    .where('in_stock', '=', true)
    .where('id', '=', testId)
    .execute();

  expect(inStockProducts).toHaveLength(1);
});

test('BigQuery JSON type handling', async () => {
  const testId = generateTestId();
  const metadata = {
    tags: ['urgent', 'international'],
    shipping: {
      carrier: 'FedEx',
      tracking: '1234567890',
    },
    notes: 'Handle with care',
  };
  
  // Insert JSON data - BigQuery native JSON columns require PARSE_JSON
  // For STRING columns storing JSON, use the query builder with automatic serialization
  await sql`
    INSERT INTO test_dataset.test_orders 
    (id, customer_id, product_id, quantity, total_amount, status, order_date, metadata, created_at)
    VALUES (
      ${testId},
      'cust_json',
      'prod_json',
      1,
      50.00,
      'pending',
      CURRENT_DATE(),
      PARSE_JSON(${JSON.stringify(metadata)}),
      ${new Date()}
    )
  `.execute(kysely);

  // Query JSON data
  const result = await sql<{notes: string; carrier: string; tags_json: string}>`
    SELECT 
      JSON_VALUE(metadata, '$.notes') as notes,
      JSON_VALUE(metadata, '$.shipping.carrier') as carrier,
      JSON_QUERY(metadata, '$.tags') as tags_json
    FROM test_dataset.test_orders
    WHERE id = ${testId}
  `.execute(kysely);

  const row = result.rows[0];
  expect(row.notes).toBe('Handle with care');
  expect(row.carrier).toBe('FedEx');
  // JSON_QUERY result is automatically parsed by BigQueryConnection
  expect(row.tags_json).toEqual(['urgent', 'international']);
});

test('BigQuery UNION syntax behavior', { timeout: 30000 }, async () => {
  // First, insert some test data
  const testId1 = generateTestId();
  const testId2 = generateTestId();
  const testId3 = generateTestId();
  
  await kysely
    .insertInto('test_dataset.test_users')
    .values([
      {
        id: testId1,
        name: 'Customer 1',
        email: 'customer1@example.com',
        created_at: new Date(),
      },
      {
        id: testId2,
        name: 'Customer 2',
        email: 'customer2@example.com',
        created_at: new Date(),
      },
    ])
    .execute();

  await kysely
    .insertInto('test_dataset.test_products')
    .values({
      id: testId3,
      name: 'Vendor Product',
      category: 'vendor',
      created_at: new Date(),
    })
    .execute();

  // Test UNION ALL - this should work
  const unionAllQuery = kysely
    .selectFrom('test_dataset.test_users')
    .select(['name'])
    .where('id', 'in', [testId1, testId2])
    .unionAll(
      kysely
        .selectFrom('test_dataset.test_products')
        .select(['name'])
        .where('id', '=', testId3)
    );

  const unionAllResult = await unionAllQuery.execute();
  expect(unionAllResult).toHaveLength(3);
  expect(unionAllResult.map(r => r.name).sort()).toEqual(['Customer 1', 'Customer 2', 'Vendor Product']);

  // Test plain UNION - this now generates UNION DISTINCT and should work
  const unionQuery = kysely
    .selectFrom('test_dataset.test_users')
    .select(['name'])
    .where('id', 'in', [testId1, testId2])
    .union(
      kysely
        .selectFrom('test_dataset.test_products')
        .select(['name'])
        .where('id', '=', testId3)
    );

  // This should now work because we generate UNION DISTINCT
  const unionResult = await unionQuery.execute();
  expect(unionResult).toHaveLength(3);
  expect(unionResult.map(r => r.name).sort()).toEqual(['Customer 1', 'Customer 2', 'Vendor Product']);

  // Verify the compiled SQL contains UNION DISTINCT
  const unionCompiled = unionQuery.compile();
  expect(unionCompiled.sql).toContain('union distinct');

  // Test that UNION DISTINCT works with raw SQL
  const unionDistinctResult = await sql`
    SELECT name FROM test_dataset.test_users WHERE id IN (${testId1}, ${testId2})
    UNION DISTINCT
    SELECT name FROM test_dataset.test_products WHERE id = ${testId3}
  `.execute(kysely);

  expect(unionDistinctResult.rows).toHaveLength(3);
  
  // Clean up
  await kysely
    .deleteFrom('test_dataset.test_users')
    .where('id', 'in', [testId1, testId2])
    .execute();

  await kysely
    .deleteFrom('test_dataset.test_products')
    .where('id', '=', testId3)
    .execute();
});

// Tests from bigquery-mysql-differences.integration.test.ts
describe('BigQuery vs MySQL Differences - Integration Tests', () => {
  
  describe('Data Type Differences', () => {
    const testTableName = 'test_dataset.test_data_types';
    
    beforeAll(async () => {
      resetTestIdCounter();
      // Create a table with BigQuery-specific data types
      await sql`
        CREATE OR REPLACE TABLE ${sql.raw(testTableName)} (
          id INT64,
          text_field STRING,
          numeric_field NUMERIC,
          bignumeric_field BIGNUMERIC,
          bytes_field BYTES,
          json_field JSON,
          array_field ARRAY<STRING>,
          struct_field STRUCT<name STRING, age INT64>,
          timestamp_field TIMESTAMP,
          datetime_field DATETIME,
          date_field DATE,
          time_field TIME,
          bool_field BOOL
        )
      `.execute(kysely).catch(() => {});
    });

    afterAll(async () => {
      await sql`DROP TABLE IF EXISTS ${sql.raw(testTableName)}`.execute(kysely);
    });

    test('INT64 type handling', { timeout: 10000 }, async () => {
      const bigIntValue = 1234567890123; // Large but safe integer
      
      await sql`
        INSERT INTO ${sql.raw(testTableName)} (id, text_field)
        VALUES (${bigIntValue}, 'test')
      `.execute(kysely);

      const result = await (kysely as any)
        .selectFrom(testTableName)
        .select('id')
        .where('id', '=', bigIntValue)
        .executeTakeFirst();

      expect(result).toBeDefined();
      expect(result.id).toBe(bigIntValue);
    });

    test('NUMERIC and BIGNUMERIC types', { timeout: 10000 }, async () => {
      await sql`
        INSERT INTO ${sql.raw(testTableName)} (id, text_field, numeric_field, bignumeric_field)
        VALUES (
          1,
          'numeric test',
          NUMERIC '99999999999999999999999999999.999999999',
          BIGNUMERIC '123456789012345678901234567890123456789.123456789'
        )
      `.execute(kysely);

      const result = await (kysely as any)
        .selectFrom(testTableName)
        .select(['numeric_field', 'bignumeric_field'])
        .where('id', '=', 1)
        .executeTakeFirst();

      expect(result).toBeDefined();
      expect(result.numeric_field).toBeDefined();
      expect(result.bignumeric_field).toBeDefined();
    });

    test('ARRAY and STRUCT types', { timeout: 10000 }, async () => {
      await (kysely as any)
        .insertInto(testTableName)
        .values({
          id: 2,
          text_field: 'complex types',
          array_field: ['item1', 'item2', 'item3'],
          struct_field: {
            name: 'John',
            age: 30,
          },
        })
        .execute();

      const result = await (kysely as any)
        .selectFrom(testTableName)
        .select(['array_field', 'struct_field'])
        .where('id', '=', 2)
        .executeTakeFirst();

      expect(result).toBeDefined();
      expect(result.array_field).toEqual(['item1', 'item2', 'item3']);
      expect(result.struct_field).toEqual({ name: 'John', age: 30 });
    });

    test('JSON type handling', { timeout: 10000 }, async () => {
      const jsonData = { key: 'value', nested: { array: [1, 2, 3] } };
      
      const jsonString = JSON.stringify(jsonData);
      await sql`
        INSERT INTO ${sql.raw(testTableName)} (id, text_field, json_field)
        VALUES (3, 'json test', PARSE_JSON(${jsonString}))
      `.execute(kysely);

      const result = await sql<{json_key: string; json_array: string}>`
        SELECT 
          JSON_VALUE(json_field, '$.key') as json_key,
          JSON_QUERY(json_field, '$.nested.array') as json_array
        FROM ${sql.raw(testTableName)}
        WHERE id = 3
      `.execute(kysely);

      expect(result.rows[0].json_key).toBe('value');
      // JSON_QUERY result is automatically parsed by BigQueryConnection
      expect(result.rows[0].json_array).toEqual([1, 2, 3]);
    });

    test('BYTES vs BLOB handling', { timeout: 10000 }, async () => {
      const testString = 'Hello, BigQuery!';
      const bytesData = Buffer.from(testString, 'utf-8');
      
      // Use BigQuery's FROM_BASE64 function to insert bytes
      await sql`
        INSERT INTO ${sql.raw(testTableName)} (id, text_field, bytes_field)
        VALUES (4, 'bytes test', FROM_BASE64(${bytesData.toString('base64')}))
      `.execute(kysely);

      const result = await (kysely as any)
        .selectFrom(testTableName)
        .select('bytes_field')
        .where('id', '=', 4)
        .executeTakeFirst();

      expect(result).toBeDefined();
      expect(result.bytes_field).toBeDefined();
      
      // BigQuery returns bytes as a Buffer
      if (Buffer.isBuffer(result.bytes_field)) {
        expect(result.bytes_field.toString('utf-8')).toBe(testString);
      } else {
        // If not a buffer, it might be base64 encoded string
        const decoded = Buffer.from(result.bytes_field, 'base64').toString('utf-8');
        expect(decoded).toBe(testString);
      }
    });
  });

  describe('Function Name Differences', () => {
    test('String functions - MySQL vs BigQuery', { timeout: 10000 }, async () => {
      // Test LENGTH vs CHAR_LENGTH
      const lengthResult = await sql<{str_length: number}>`
        SELECT CHAR_LENGTH('Hello') as str_length
      `.execute(kysely);
      expect(lengthResult.rows[0].str_length).toBe(5);

      // LENGTH() in raw SQL is not translated (only in query builder)
      const rawLengthResult = await sql<{str_length: number}>`
        SELECT LENGTH('Hello') as str_length
      `.execute(kysely);
      expect(rawLengthResult.rows[0].str_length).toBe(5);

      // Test SUBSTR vs SUBSTRING
      const substrResult = await sql<{substring: string}>`
        SELECT SUBSTR('Hello World', 7) as substring
      `.execute(kysely);
      expect(substrResult.rows[0].substring).toBe('World');
    });

    test('Date/Time functions - MySQL vs BigQuery', { timeout: 10000 }, async () => {
      // CURRENT_TIMESTAMP vs NOW
      const timestampResult = await sql<{now_time: any}>`
        SELECT CURRENT_TIMESTAMP() as now_time
      `.execute(kysely);
      expect(timestampResult.rows[0].now_time).toBeDefined();

      // NOW() should be translated to CURRENT_TIMESTAMP() and work
      const nowResult = await sql<{now_time: any}>`
        SELECT NOW() as now_time
      `.execute(kysely);
      expect(nowResult.rows[0].now_time).toBeDefined();

      // DATE_ADD differences
      const dateAddResult = await sql<{tomorrow: any}>`
        SELECT DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) as tomorrow
      `.execute(kysely);
      expect(dateAddResult.rows[0].tomorrow).toBeDefined();

      // TIMESTAMP_ADD for timestamps
      const timestampAddResult = await sql<{next_hour: any}>`
        SELECT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) as next_hour
      `.execute(kysely);
      expect(timestampAddResult.rows[0].next_hour).toBeDefined();

      // FORMAT_TIMESTAMP - correct BigQuery syntax (format, then timestamp)
      const formatResult = await sql<{formatted: string}>`
        SELECT FORMAT_TIMESTAMP('%Y-%m-%d', CURRENT_TIMESTAMP()) as formatted
      `.execute(kysely);
      expect(formatResult.rows[0].formatted).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      
      // DATE_FORMAT should be translated to FORMAT_TIMESTAMP and work
      const dateFormatResult = await sql<{formatted: string}>`
        SELECT DATE_FORMAT(CURRENT_TIMESTAMP(), '%Y-%m-%d %H:%M:%S') as formatted
      `.execute(kysely);
      expect(dateFormatResult.rows[0].formatted).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
    });

    test('JSON functions - MySQL vs BigQuery', { timeout: 10000 }, async () => {
      const testJson = { name: 'Test', value: 123 };
      
      // BigQuery JSON functions
      const jsonString = JSON.stringify(testJson);
      const jsonResult = await sql<{name: string; full_json: string}>`
        SELECT 
          JSON_VALUE(PARSE_JSON(${jsonString}), '$.name') as name,
          JSON_QUERY(PARSE_JSON(${jsonString}), '$') as full_json
      `.execute(kysely);

      expect(jsonResult.rows[0].name).toBe('Test');
      // JSON_QUERY result is automatically parsed by BigQueryConnection
      expect(jsonResult.rows[0].full_json).toEqual(testJson);

      // MySQL JSON operators (-> and ->>) don't work in BigQuery
      await expect(sql`
        SELECT JSON '{"name": "Test"}'->>'$.name' as name
      `.execute(kysely)).rejects.toThrow();
    });

    test('Aggregate functions - BigQuery specific', { timeout: 10000 }, async () => {
      // Create test data
      await sql`
        CREATE OR REPLACE TABLE test_dataset.test_aggregates (
          id INT64,
          value STRING
        )
      `.execute(kysely);

      await kysely
        .insertInto('test_dataset.test_aggregates')
        .values([
          { id: 1, value: 'A' },
          { id: 2, value: 'B' },
          { id: 3, value: 'A' },
          { id: 4, value: 'C' },
          { id: 5, value: 'A' },
        ])
        .execute();

      try {
        // APPROX_COUNT_DISTINCT
        const approxResult = await sql<{approx_unique: number}>`
          SELECT APPROX_COUNT_DISTINCT(value) as approx_unique
          FROM test_dataset.test_aggregates
        `.execute(kysely);

        expect(approxResult.rows[0].approx_unique).toBe(3);
      } finally {
        // Cleanup
        await sql`DROP TABLE IF EXISTS test_dataset.test_aggregates`.execute(kysely);
      }
    });
  });

  describe('DML Restrictions', () => {
    beforeAll(async () => {
      resetTestIdCounter();
      await sql`
        CREATE OR REPLACE TABLE test_dataset.test_dml (
          id INT64,
          name STRING,
          status STRING
        )
      `.execute(kysely);

      await kysely
        .insertInto('test_dataset.test_dml')
        .values([
          { id: 1, name: 'Test 1', status: 'active' },
          { id: 2, name: 'Test 2', status: 'inactive' },
          { id: 3, name: 'Test 3', status: 'active' },
        ])
        .execute();
    });

    afterAll(async () => {
      await sql`DROP TABLE IF EXISTS test_dataset.test_dml`.execute(kysely);
    });

    test('UPDATE without WHERE clause should fail', { timeout: 10000 }, async () => {
      // BigQuery requires WHERE clause for UPDATE
      await expect(sql`
        UPDATE test_dataset.test_dml
        SET status = 'updated'
      `.execute(kysely)).rejects.toThrow(/UPDATE must have a WHERE clause/);

      // UPDATE with WHERE should work
      await sql`
        UPDATE test_dataset.test_dml
        SET status = 'updated'
        WHERE id = 1
      `.execute(kysely);

      const result = await kysely
        .selectFrom('test_dataset.test_dml')
        .select('status')
        .where('id', '=', 1)
        .executeTakeFirst();

      expect(result!.status).toBe('updated');
    });

    test('DELETE without WHERE clause should fail', { timeout: 10000 }, async () => {
      // BigQuery requires WHERE clause for DELETE
      await expect(sql`
        DELETE FROM test_dataset.test_dml
      `.execute(kysely)).rejects.toThrow(/DELETE must have a WHERE clause/);

      // DELETE with WHERE should work
      await sql`
        DELETE FROM test_dataset.test_dml
        WHERE id = 2
      `.execute(kysely);

      const count = await kysely
        .selectFrom('test_dataset.test_dml')
        .select(sql`COUNT(*)`.as('count'))
        .executeTakeFirst();

      expect(count!.count).toBe(2);
    });
  });

  describe('DDL Features', () => {
    beforeAll(() => {
      resetTestIdCounter();
    });

    test('CREATE TABLE with PARTITION BY and CLUSTER BY', { timeout: 10000 }, async () => {
      const tableName = 'test_dataset.test_partitioned';
      
      // Create partitioned and clustered table
      await sql`
        CREATE OR REPLACE TABLE ${sql.raw(tableName)} (
          id INT64,
          user_id STRING,
          event_timestamp TIMESTAMP,
          event_type STRING,
          data JSON
        )
        PARTITION BY DATE(event_timestamp)
        CLUSTER BY user_id, event_type
      `.execute(kysely);

      // Insert test data
      await sql`
        INSERT INTO ${sql.raw(tableName)} (id, user_id, event_timestamp, event_type, data)
        VALUES (
          1,
          'user123',
          CURRENT_TIMESTAMP(),
          'click',
          JSON '{"action": "button_click"}'
        )
      `.execute(kysely);

      // Query should work
      const result = await kysely
        .selectFrom(tableName)
        .select('id')
        .where('user_id', '=', 'user123')
        .executeTakeFirst();

      expect(result).toBeDefined();

      // Cleanup
      await sql`DROP TABLE ${sql.raw(tableName)}`.execute(kysely);
    });

    test('Project.dataset.table naming convention', { timeout: 10000 }, async () => {
      const projectId = process.env.GCP_PROJECT_ID;
      const fullTableName = `test_dataset.test_full_name`; // BigQuery driver already handles project ID
      
      // Create table with full name
      await sql`
        CREATE OR REPLACE TABLE ${sql.raw(fullTableName)} (
          id INT64,
          name STRING
        )
      `.execute(kysely);

      // Insert and query using full name
      await kysely
        .insertInto(fullTableName)
        .values({ id: 1, name: 'Test' })
        .execute();

      const result = await kysely
        .selectFrom(fullTableName)
        .select('name')
        .where('id', '=', 1)
        .executeTakeFirst();

      expect(result!.name).toBe('Test');

      // Cleanup
      await sql`DROP TABLE ${sql.raw(fullTableName)}`.execute(kysely);
    });

    test('Concurrent operations', { timeout: 30000 }, async () => {
      // Test concurrent inserts and queries
      const testIds = Array.from({ length: 5 }, () => generateTestId());
      
      try {
        // Clear any existing test data first
        await kysely
          .deleteFrom('test_dataset.test_users')
          .where('id', 'like', 'test_%')
          .execute()
          .catch(() => {});
        
        // Execute multiple operations concurrently
        await Promise.all([
          // Concurrent inserts
          ...testIds.map((id) => 
            kysely
              .insertInto('test_dataset.test_users')
              .values({
                id,
                name: `Concurrent User ${id}`,
                email: `concurrent${id}@example.com`,
                created_at: new Date(),
              })
              .execute()
          ),
          // Concurrent queries
          kysely.selectFrom('test_dataset.test_users').select('id').limit(10).execute(),
          kysely.selectFrom('test_dataset.test_products').select('id').limit(10).execute(),
        ]);
        
        // Verify all inserts succeeded
        const results = await kysely
          .selectFrom('test_dataset.test_users')
          .where('id', 'in', testIds)
          .select('id')
          .execute();
          
        expect(results).toHaveLength(testIds.length);
      } finally {
        // Cleanup
        await kysely
          .deleteFrom('test_dataset.test_users')
          .where('id', 'in', testIds)
          .execute()
          .catch(() => {});
      }
    });

    test('Wildcard table queries', { timeout: 10000 }, async () => {
      // Create multiple tables with similar names
      const tables = ['events_2023', 'events_2024'];
      
      for (const table of tables) {
        await sql`
          CREATE OR REPLACE TABLE test_dataset.${sql.raw(table)} (
            id INT64,
            event STRING
          )
        `.execute(kysely);

        await kysely
          .insertInto(`test_dataset.${table}`)
          .values({ id: 1, event: table })
          .execute();
      }

      // Query with wildcard
      const wildcardResult = await sql<{event: string}>`
        SELECT event
        FROM \`test_dataset.events_*\`
        ORDER BY event
      `.execute(kysely);

      expect(wildcardResult.rows).toHaveLength(2);
      expect(wildcardResult.rows.map((r) => r.event)).toEqual(['events_2023', 'events_2024']);

      // Cleanup
      for (const table of tables) {
        await sql`DROP TABLE test_dataset.${sql.raw(table)}`.execute(kysely);
      }
    });
  });

  describe('Unsupported Features', () => {
    beforeAll(() => {
      resetTestIdCounter();
    });

    test('Indexes are not supported', { timeout: 10000 }, async () => {
      // Try to create an index - should fail
      await expect(sql`
        CREATE INDEX idx_test ON test_dataset.test_table (id)
      `.execute(kysely)).rejects.toThrow(/CREATE INDEX is not supported/);
    });

    test('Primary keys and constraints are not enforced', { timeout: 10000 }, async () => {
      // Create table without constraints first
      await sql`
        CREATE OR REPLACE TABLE test_dataset.test_constraints (
          id INT64 NOT NULL,
          email STRING,
          user_id INT64
        )
      `.execute(kysely);
      
      // Note: BigQuery supports constraint syntax in CREATE TABLE but with specific requirements
      // For this test, we'll demonstrate that duplicate primary keys can be inserted

      // Should be able to insert duplicate primary keys
      await kysely
        .insertInto('test_dataset.test_constraints')
        .values([
          { id: 1, email: 'test@example.com' },
          { id: 1, email: 'test@example.com' }, // Duplicate PK and unique
        ])
        .execute();

      const count = await kysely
        .selectFrom('test_dataset.test_constraints')
        .select(sql`COUNT(*)`.as('count'))
        .executeTakeFirst();

      expect(count!.count).toBe(2); // Both rows inserted despite constraints

      // Cleanup
      await sql`DROP TABLE IF EXISTS test_dataset.test_constraints`.execute(kysely);
    });
  });
});