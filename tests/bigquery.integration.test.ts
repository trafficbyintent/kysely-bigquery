import {expect, test, beforeAll, afterAll} from 'vitest';
import {Kysely, sql} from 'kysely';

import {BigQueryDialect} from '../src';
import {createBigQueryInstance} from './config';
import {
  generateTestId,
  createTestTable,
  cleanupTestTable,
  TEST_USERS_SCHEMA,
  TEST_PRODUCTS_SCHEMA,
  TEST_ORDERS_SCHEMA,
} from './helpers';

const kysely = new Kysely<any>({
  dialect: new BigQueryDialect({
    bigquery: createBigQueryInstance(),
  }),
});

// Setup test tables before running tests
beforeAll(async () => {
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
  
  // Since the result has BigQueryTimestamp objects, we'll just verify the structure
  const row = rows[0];
  expect(row.id).toBe(28);
  expect(row.category).toBe('cac');
  expect(row.name).toBe('cac');
  expect(row.created_at).toHaveProperty('value');
  expect(row.updated_at).toHaveProperty('value');
  expect(row.inserted_at).toHaveProperty('value');
});

test('introspection', async () => {
  const tables = await kysely.introspection.getTables();

  expect(tables.filter(t => t.name === 'bank_account_transactions')).toMatchSnapshot();
});

test('INSERT operations', async () => {
  // Test basic insert
  const insertResult = await kysely
    .insertInto('test_dataset.test_users')
    .values({
      id: generateTestId(),
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
  const testId1 = generateTestId();
  const testId2 = generateTestId();
  
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

test('UPDATE operations', async () => {
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

test('BigQuery ARRAY type handling', async () => {
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
  const hasTag = await sql`
    SELECT EXISTS(
      SELECT 1 
      FROM test_dataset.test_users 
      WHERE id = ${testId}
      AND 'typescript' IN UNNEST(tags)
    ) as has_tag
  `.execute(kysely);

  expect(hasTag.rows[0].has_tag).toBe(true);

  // Test ARRAY_LENGTH
  const arrayLength = await sql`
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
  const structField = await sql`
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
  const result = await sql`
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
  const dateDiff = await sql`
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
  
  // Insert JSON data
  await sql`
    INSERT INTO test_dataset.test_orders (
      id, customer_id, product_id, quantity, total_amount, 
      status, order_date, metadata, created_at
    )
    VALUES (
      ${testId}, 'cust_json', 'prod_json', 1, 50.00,
      'pending', CURRENT_DATE(), 
      PARSE_JSON(${JSON.stringify(metadata)}),
      CURRENT_TIMESTAMP()
    )
  `.execute(kysely);

  // Query JSON data
  const result = await sql`
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
  expect(JSON.parse(row.tags_json)).toEqual(['urgent', 'international']);
});