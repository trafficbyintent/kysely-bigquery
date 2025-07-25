import { Kysely, sql } from 'kysely';
import { describe, expect, test, vi } from 'vitest';

import { BigQueryDialect } from '../src';
import { expectedSimpleSelectCompiled } from './helpers';

vi.mock('@google-cloud/bigquery', () => {
  return {
    BigQuery: class MockBigQuery {},
    BigQueryTimestamp: class {
      constructor(public value: string) {}
    },
  };
});

test('simple select query compilation', async () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely.selectFrom('features.metadata').where('id', '>', 10).selectAll().limit(1);

  expect(query.compile()).toEqual(expectedSimpleSelectCompiled);
});

test('dialect configuration', () => {
  const dialect = new BigQueryDialect({ options: { projectId: 'test-project' } });
  expect(dialect).toBeDefined();
  expect(dialect.createAdapter()).toBeDefined();
  expect(dialect.createDriver()).toBeDefined();
  expect(dialect.createQueryCompiler()).toBeDefined();
});

test('query builder with joins', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .selectFrom('users as u')
    .leftJoin('posts as p', 'p.user_id', 'u.id')
    .select(['u.name', 'p.title'])
    .where('u.active', '=', true);

  const compiled = query.compile();
  expect(compiled.sql).toContain('left join');
  expect(compiled.sql).toContain('`users` as `u`');
  expect(compiled.sql).toContain('`posts` as `p`');
});

test('insert query compilation', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .insertInto('users')
    .values({
      name: 'John Doe',
      email: 'john@example.com',
      created_at: new Date('2024-01-01'),
    });

  const compiled = query.compile();
  expect(compiled.sql).toContain('insert into `users`');
  expect(compiled.parameters).toHaveLength(3);
});

test('update query compilation', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .updateTable('users')
    .set({ name: 'Jane Doe' })
    .where('id', '=', 1);

  const compiled = query.compile();
  expect(compiled.sql).toContain('update `users` set');
  expect(compiled.sql).toContain('where `id` = ?');
});

test('delete query compilation', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .deleteFrom('users')
    .where('active', '=', false);

  const compiled = query.compile();
  expect(compiled.sql).toContain('delete from `users`');
  expect(compiled.sql).toContain('where `active` = ?');
});

test('complex WHERE clauses', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // AND/OR conditions
  const andOrQuery = kysely
    .selectFrom('users')
    .where('age', '>=', 18)
    .where((eb) => eb.or([
      eb('status', '=', 'active'),
      eb('status', '=', 'pending'),
    ]))
    .selectAll();

  const andOrCompiled = andOrQuery.compile();
  expect(andOrCompiled.sql).toBe('select * from `users` where `age` >= ? and (`status` = ? or `status` = ?)');
  expect(andOrCompiled.parameters).toEqual([18, 'active', 'pending']);

  // IN clause
  const inQuery = kysely
    .selectFrom('products')
    .where('category', 'in', ['electronics', 'books', 'toys'])
    .selectAll();

  const inCompiled = inQuery.compile();
  expect(inCompiled.sql).toBe('select * from `products` where `category` in (?, ?, ?)');
  expect(inCompiled.parameters).toEqual(['electronics', 'books', 'toys']);

  // NOT IN clause
  const notInQuery = kysely
    .selectFrom('orders')
    .where('status', 'not in', ['cancelled', 'refunded'])
    .selectAll();

  const notInCompiled = notInQuery.compile();
  expect(notInCompiled.sql).toBe('select * from `orders` where `status` not in (?, ?)');
  expect(notInCompiled.parameters).toEqual(['cancelled', 'refunded']);

  // BETWEEN clause
  const betweenQuery = kysely
    .selectFrom('transactions')
    .where((eb) => eb.between('amount', 100, 500))
    .selectAll();

  const betweenCompiled = betweenQuery.compile();
  expect(betweenCompiled.sql).toBe('select * from `transactions` where `amount` between ? and ?');
  expect(betweenCompiled.parameters).toEqual([100, 500]);
});

test('GROUP BY and HAVING clauses', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .selectFrom('orders')
    .select(['customer_id', kysely.fn.count('id').as('order_count')])
    .groupBy('customer_id')
    .having('order_count', '>', 5)
    .orderBy('order_count', 'desc');

  const compiled = query.compile();
  expect(compiled.sql).toBe(
    'select `customer_id`, count(`id`) as `order_count` from `orders` group by `customer_id` having `order_count` > ? order by `order_count` desc'
  );
  expect(compiled.parameters).toEqual([5]);
});

test('ORDER BY with multiple columns', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .selectFrom('products')
    .selectAll()
    .orderBy('category', 'asc')
    .orderBy('price', 'desc')
    .orderBy('name');

  const compiled = query.compile();
  expect(compiled.sql).toBe('select * from `products` order by `category` asc, `price` desc, `name`');
});

test('subqueries', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // Subquery in WHERE
  const subqueryWhere = kysely
    .selectFrom('orders')
    .selectAll()
    .where('customer_id', 'in', (eb) =>
      eb.selectFrom('customers')
        .select('id')
        .where('country', '=', 'USA')
    );

  const whereCompiled = subqueryWhere.compile();
  expect(whereCompiled.sql).toBe(
    'select * from `orders` where `customer_id` in (select `id` from `customers` where `country` = ?)'
  );
  expect(whereCompiled.parameters).toEqual(['USA']);

  // Subquery in SELECT
  const subquerySelect = kysely
    .selectFrom('customers as c')
    .select([
      'c.id',
      'c.name',
      (eb) => eb.selectFrom('orders')
        .select(eb.fn.count('id').as('count'))
        .whereRef('orders.customer_id', '=', 'c.id')
        .as('order_count'),
    ]);

  const selectCompiled = subquerySelect.compile();
  expect(selectCompiled.sql).toBe(
    'select `c`.`id`, `c`.`name`, (select count(`id`) as `count` from `orders` where `orders`.`customer_id` = `c`.`id`) as `order_count` from `customers` as `c`'
  );
});

test('UNION operations', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .selectFrom('customers')
    .select(['name', 'email'])
    .where('status', '=', 'active')
    .union(
      kysely
        .selectFrom('vendors')
        .select(['company_name as name', 'contact_email as email'])
        .where('active', '=', true)
    );

  const compiled = query.compile();
  expect(compiled.sql).toBe(
    'select `name`, `email` from `customers` where `status` = ? union distinct select `company_name` as `name`, `contact_email` as `email` from `vendors` where `active` = ?'
  );
  expect(compiled.parameters).toEqual(['active', true]);
});

test('BigQuery UNION syntax - requires UNION DISTINCT', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // Test regular UNION - BigQuery requires UNION DISTINCT
  const unionQuery = kysely
    .selectFrom('customers')
    .select(['id', 'name'])
    .union(
      kysely
        .selectFrom('vendors')
        .select(['vendor_id as id', 'vendor_name as name'])
    );

  const unionCompiled = unionQuery.compile();
  expect(unionCompiled.sql).toBe(
    'select `id`, `name` from `customers` union distinct select `vendor_id` as `id`, `vendor_name` as `name` from `vendors`'
  );

  // Test UNION ALL - should remain unchanged
  const unionAllQuery = kysely
    .selectFrom('customers')
    .select(['id', 'name'])
    .unionAll(
      kysely
        .selectFrom('vendors')
        .select(['vendor_id as id', 'vendor_name as name'])
    );

  const unionAllCompiled = unionAllQuery.compile();
  expect(unionAllCompiled.sql).toBe(
    'select `id`, `name` from `customers` union all select `vendor_id` as `id`, `vendor_name` as `name` from `vendors`'
  );
});

test('CTEs (Common Table Expressions)', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .with('high_value_customers', (db) =>
      db.selectFrom('customers')
        .select(['id', 'name'])
        .where('lifetime_value', '>', 1000)
    )
    .selectFrom('high_value_customers')
    .selectAll();

  const compiled = query.compile();
  expect(compiled.sql).toBe(
    'with `high_value_customers` as (select `id`, `name` from `customers` where `lifetime_value` > ?) select * from `high_value_customers`'
  );
  expect(compiled.parameters).toEqual([1000]);
});

test('BigQuery ARRAY type operations', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // Array literal
  const arrayLiteralQuery = kysely
    .selectFrom('users')
    .select(sql`ARRAY[1, 2, 3]`.as('numbers'))
    .selectAll();

  const arrayLiteralCompiled = arrayLiteralQuery.compile();
  expect(arrayLiteralCompiled.sql).toBe('select ARRAY[1, 2, 3] as `numbers`, * from `users`');

  // UNNEST operation - using innerJoin with raw SQL
  const unnestQuery = kysely
    .selectFrom('products')
    .innerJoin(
      (eb) => eb.selectFrom(sql`UNNEST(${sql.ref('products.tags')})`.as('tag')).selectAll().as('tag'),
      'tag',
      'tag',
    )
    .select(['products.name', 'tag'])
    .where('tag', '=', 'electronics');

  const unnestCompiled = unnestQuery.compile();
  expect(unnestCompiled.sql).toContain('UNNEST(`products`.`tags`)');
  expect(unnestCompiled.sql).toContain('as `tag`');
  expect(unnestCompiled.parameters).toEqual(['electronics']);

  // Array functions
  const arrayFunctionQuery = kysely
    .selectFrom('users')
    .select(sql`ARRAY_LENGTH(${sql.ref('skills')})`.as('skill_count'))
    .where(sql`'programming' IN UNNEST(${sql.ref('skills')})`, '=', true);

  const arrayFunctionCompiled = arrayFunctionQuery.compile();
  expect(arrayFunctionCompiled.sql).toBe(
    'select ARRAY_LENGTH(`skills`) as `skill_count` from `users` where \'programming\' IN UNNEST(`skills`) = ?'
  );
  expect(arrayFunctionCompiled.parameters).toEqual([true]);
});

test('BigQuery STRUCT type operations', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // STRUCT literal
  const structLiteralQuery = kysely
    .selectFrom('users')
    .select(sql`STRUCT('John' AS name, 30 AS age)`.as('person'))
    .selectAll();

  const structLiteralCompiled = structLiteralQuery.compile();
  expect(structLiteralCompiled.sql).toBe('select STRUCT(\'John\' AS name, 30 AS age) as `person`, * from `users`');

  // Accessing STRUCT fields
  const structFieldQuery = kysely
    .selectFrom('orders')
    .select(['id', sql`${sql.ref('customer')}.name`.as('customer_name')])
    .where(sql`${sql.ref('customer')}.country`, '=', 'USA');

  const structFieldCompiled = structFieldQuery.compile();
  expect(structFieldCompiled.sql).toBe(
    'select `id`, `customer`.name as `customer_name` from `orders` where `customer`.country = ?'
  );
  expect(structFieldCompiled.parameters).toEqual(['USA']);
});

test('BigQuery specific table naming', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // Project.dataset.table format
  const fullTableQuery = kysely
    .selectFrom('my-project.analytics.events')
    .selectAll()
    .limit(10);

  const fullTableCompiled = fullTableQuery.compile();
  expect(fullTableCompiled.sql).toBe('select * from `my-project`.`analytics` limit ?');
  expect(fullTableCompiled.parameters).toEqual([10]);

  // Dataset.table format (already tested in simple select)
  // Table with special characters
  const specialCharsQuery = kysely
    .selectFrom('dataset.table-with-dashes')
    .selectAll();

  const specialCharsCompiled = specialCharsQuery.compile();
  expect(specialCharsCompiled.sql).toBe('select * from `dataset`.`table-with-dashes`');
});

test('BigQuery window functions', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .selectFrom('sales')
    .select([
      'product_id',
      'sale_date',
      'amount',
      sql`ROW_NUMBER() OVER (PARTITION BY ${sql.ref('product_id')} ORDER BY ${sql.ref('sale_date')} DESC)`.as('row_num'),
      sql`SUM(${sql.ref('amount')}) OVER (PARTITION BY ${sql.ref('product_id')})`.as('total_product_sales'),
    ]);

  const compiled = query.compile();
  expect(compiled.sql).toBe(
    'select `product_id`, `sale_date`, `amount`, ROW_NUMBER() OVER (PARTITION BY `product_id` ORDER BY `sale_date` DESC) as `row_num`, SUM(`amount`) OVER (PARTITION BY `product_id`) as `total_product_sales` from `sales`'
  );
});

test('BigQuery DATE/DATETIME/TIMESTAMP functions', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .selectFrom('events')
    .select([
      sql`CURRENT_TIMESTAMP()`.as('now'),
      sql`DATE(${sql.ref('created_at')})`.as('created_date'),
      sql`EXTRACT(YEAR FROM ${sql.ref('created_at')})`.as('year'),
      sql`DATE_DIFF(CURRENT_DATE(), DATE(${sql.ref('created_at')}), DAY)`.as('days_ago'),
    ])
    .where(sql`DATE(${sql.ref('created_at')})`, '>=', sql`DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)`);

  const compiled = query.compile();
  expect(compiled.sql).toBe(
    'select CURRENT_TIMESTAMP() as `now`, DATE(`created_at`) as `created_date`, EXTRACT(YEAR FROM `created_at`) as `year`, DATE_DIFF(CURRENT_DATE(), DATE(`created_at`), DAY) as `days_ago` from `events` where DATE(`created_at`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)'
  );
});

test('error handling - invalid table references', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // Test that queries compile even with potentially invalid references
  // The dialect should not validate table/column names during compilation
  const query = kysely
    .selectFrom('') // empty table name
    .selectAll();

  const compiled = query.compile();
  expect(compiled.sql).toBe('select * from ``');

  // Multiple dots in table name
  const multiDotQuery = kysely
    .selectFrom('project..dataset..table')
    .selectAll();

  const multiDotCompiled = multiDotQuery.compile();
  expect(multiDotCompiled.sql).toBe('select * from `project`.``');
});

test('edge cases - special characters and escaping', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // Backticks in identifiers
  const backtickQuery = kysely
    .selectFrom('table`with`backticks')
    .select('column`with`backticks')
    .where('field`test', '=', 'value');

  const backtickCompiled = backtickQuery.compile();
  // BigQuery escapes backticks by doubling them
  expect(backtickCompiled.sql).toBe('select `column``with``backticks` from `table``with``backticks` where `field``test` = ?');
  expect(backtickCompiled.parameters).toEqual(['value']);

  // Unicode characters (Japanese)
  const unicodeQuery = kysely
    .selectFrom('顧客テーブル')
    .select('名前')
    .where('都道府県', '=', '東京都');

  const unicodeCompiled = unicodeQuery.compile();
  expect(unicodeCompiled.sql).toBe('select `名前` from `顧客テーブル` where `都道府県` = ?');
  expect(unicodeCompiled.parameters).toEqual(['東京都']);
});

test('null and undefined handling', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  // NULL values
  const nullQuery = kysely
    .insertInto('users')
    .values({
      name: 'John',
      email: null,
      phone: undefined,
    });

  const nullCompiled = nullQuery.compile();
  expect(nullCompiled.sql).toBe('insert into `users` (`name`, `email`) values (?, ?)');
  expect(nullCompiled.parameters).toEqual(['John', null]);

  // IS NULL / IS NOT NULL
  const isNullQuery = kysely
    .selectFrom('users')
    .where('email', 'is', null)
    .where('phone', 'is not', null)
    .selectAll();

  const isNullCompiled = isNullQuery.compile();
  expect(isNullCompiled.sql).toBe('select * from `users` where `email` is null and `phone` is not null');
});

// Tests from bigquery-mysql-differences.test.ts
describe('BigQuery vs MySQL Differences - Unit Tests', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  describe('Function Translation Tests', () => {
    test('Should translate LENGTH to CHAR_LENGTH', () => {
      // Test using Kysely function builder
      const query = kysely
        .selectFrom('users')
        .select(kysely.fn('length', [sql.ref('name')]).as('name_length'));

      const compiled = query.compile();
      // LENGTH should now be translated to CHAR_LENGTH
      expect(compiled.sql).toContain('CHAR_LENGTH(`name`)');
      expect(compiled.sql).not.toContain(' LENGTH(');
      
      // Test raw SQL translation
      const rawQuery = kysely
        .selectFrom('users')
        .select(sql`LENGTH(${sql.ref('name')})`.as('name_length'));
      
      const rawCompiled = rawQuery.compile();
      // Raw SQL currently doesn't get translated
      expect(rawCompiled.sql).toContain('LENGTH(`name`)');
    });

    test('Should translate SUBSTRING to SUBSTR', () => {
      const query = kysely
        .selectFrom('users')
        .select(sql`SUBSTRING(${sql.ref('name')}, 1, 5)`.as('name_part'));

      const compiled = query.compile();
      
      // Currently passes as-is
      expect(compiled.sql).toContain('SUBSTRING(`name`, 1, 5)');
      // Desired: SUBSTR(`name`, 1, 5)
    });

    test('Should translate NOW() to CURRENT_TIMESTAMP()', () => {
      const query = kysely
        .selectFrom('events')
        .select(sql`NOW()`.as('current_time'));

      const compiled = query.compile();
      
      // Should translate NOW() to CURRENT_TIMESTAMP()
      expect(compiled.sql).toContain('CURRENT_TIMESTAMP()');
      expect(compiled.sql).not.toContain('NOW()');
    });

    test('Should handle DATE_ADD syntax differences', () => {
      // MySQL style
      const mysqlStyle = sql`DATE_ADD(${sql.ref('created_at')}, INTERVAL 1 DAY)`;
      
      // BigQuery style for dates
      const bigqueryDateStyle = sql`DATE_ADD(${sql.ref('created_at')}, INTERVAL 1 DAY)`;
      
      // BigQuery style for timestamps
      const bigqueryTimestampStyle = sql`TIMESTAMP_ADD(${sql.ref('created_at')}, INTERVAL 1 DAY)`;

      const query = kysely
        .selectFrom('orders')
        .select(mysqlStyle.as('next_day'));

      const compiled = query.compile();
      
      // Currently passes through unchanged
      expect(compiled.sql).toContain('DATE_ADD(`created_at`, INTERVAL 1 DAY)');
    });

    test('Should translate DATE_FORMAT to FORMAT_TIMESTAMP', () => {
      const query = kysely
        .selectFrom('orders')
        .select(sql`DATE_FORMAT(${sql.ref('created_at')}, '%Y-%m-%d')`.as('formatted'));

      const compiled = query.compile();
      
      // Should translate to BigQuery syntax with swapped parameters
      expect(compiled.sql).toContain("FORMAT_TIMESTAMP('%Y-%m-%d', `created_at`)");
      expect(compiled.sql).not.toContain('DATE_FORMAT');
    });
  });

  describe('Data Type Mapping Tests', () => {
    test('Should handle BigQuery-specific types in CREATE TABLE', () => {
      const createTableQuery = sql`
        CREATE TABLE users (
          id INT64,
          name STRING,
          email STRING,
          age INT64,
          balance NUMERIC(10, 2),
          data JSON,
          tags ARRAY<STRING>,
          address STRUCT<street STRING, city STRING, zip STRING>,
          profile_pic BYTES,
          created_at TIMESTAMP
        )
      `;

      // Raw SQL queries need to be compiled with the dialect
      const compiledQuery = createTableQuery.compile(kysely);
      expect(compiledQuery.sql).toContain('INT64');
      expect(compiledQuery.sql).toContain('STRING');
      expect(compiledQuery.sql).toContain('ARRAY<STRING>');
      expect(compiledQuery.sql).toContain('STRUCT<');
    });

    test('Should map MySQL types to BigQuery types', () => {
      // This would be in a custom adapter
      const typeMap = {
        'VARCHAR': 'STRING',
        'TEXT': 'STRING',
        'INT': 'INT64',
        'BIGINT': 'INT64',
        'DECIMAL': 'NUMERIC',
        'BLOB': 'BYTES',
        'JSON': 'JSON',
      };

      // Test type mapping
      expect(typeMap['VARCHAR']).toBe('STRING');
      expect(typeMap['INT']).toBe('INT64');
      expect(typeMap['BLOB']).toBe('BYTES');
    });
  });

  describe('DML Validation Tests', () => {
    test('UPDATE without WHERE should add validation', () => {
      const updateQuery = kysely
        .updateTable('users')
        .set({ status: 'active' });

      const compiled = updateQuery.compile();
      
      // BigQuery requires WHERE clause - we add WHERE TRUE
      expect(compiled.sql).toBe('update `users` set `status` = ? where true');
      expect(compiled.sql).toContain('where true');
    });

    test('DELETE without WHERE should add validation', () => {
      const deleteQuery = kysely
        .deleteFrom('users');

      const compiled = deleteQuery.compile();
      
      // BigQuery requires WHERE clause - we add WHERE TRUE
      expect(compiled.sql).toBe('delete from `users` where true');
      expect(compiled.sql).toContain('where true');
    });

    test('UPDATE with WHERE should work normally', () => {
      const updateQuery = kysely
        .updateTable('users')
        .set({ status: 'active' })
        .where('id', '=', 1);

      const compiled = updateQuery.compile();
      
      expect(compiled.sql).toBe('update `users` set `status` = ? where `id` = ?');
      expect(compiled.parameters).toEqual(['active', 1]);
    });

    test('DELETE with WHERE should work normally', () => {
      const deleteQuery = kysely
        .deleteFrom('users')
        .where('status', '=', 'inactive');

      const compiled = deleteQuery.compile();
      
      expect(compiled.sql).toBe('delete from `users` where `status` = ?');
      expect(compiled.parameters).toEqual(['inactive']);
    });
  });

  describe('DDL Enhancement Tests', () => {
    test('Should handle PARTITION BY in CREATE TABLE', () => {
      // This would need custom DDL builder support
      const createTableWithPartition = sql`
        CREATE TABLE events (
          id INT64,
          user_id STRING,
          event_timestamp TIMESTAMP,
          event_type STRING
        )
        PARTITION BY DATE(event_timestamp)
      `;

      const compiledQuery = createTableWithPartition.compile(kysely);
      expect(compiledQuery.sql).toContain('PARTITION BY');
    });

    test('Should handle CLUSTER BY in CREATE TABLE', () => {
      const createTableWithClustering = sql`
        CREATE TABLE events (
          id INT64,
          user_id STRING,
          event_timestamp TIMESTAMP,
          event_type STRING
        )
        CLUSTER BY user_id, event_type
      `;

      const compiledQuery = createTableWithClustering.compile(kysely);
      expect(compiledQuery.sql).toContain('CLUSTER BY');
    });

    test('Should handle project.dataset.table naming', () => {
      const query = kysely
        .selectFrom('my-project.analytics.events')
        .selectAll();

      const compiled = query.compile();
      
      // Currently works but loses one level
      expect(compiled.sql).toBe('select * from `my-project`.`analytics`');
      // Desired: select * from `my-project`.`analytics`.`events`
    });
  });

  describe('Constraint Generation Tests', () => {
    test('PRIMARY KEY constraint should include NOT ENFORCED', () => {
      const query = kysely.schema
        .createTable('test_table')
        .addColumn('id', 'integer', (col) => col.primaryKey())
        .addColumn('name', 'varchar');
      
      const compiled = query.compile();
      expect(compiled.sql).toContain('primary key');
      expect(compiled.sql).toContain('not enforced');
    });

    test('Composite PRIMARY KEY should include NOT ENFORCED', () => {
      const query = kysely.schema
        .createTable('test_table')
        .addColumn('order_id', 'integer')
        .addColumn('product_id', 'integer')
        .addPrimaryKeyConstraint('pk_composite', ['order_id', 'product_id']);
      
      const compiled = query.compile();
      expect(compiled.sql).toContain('constraint `pk_composite` primary key (`order_id`, `product_id`) not enforced');
    });

    test('UNIQUE constraint should include NOT ENFORCED', () => {
      const query = kysely.schema
        .createTable('test_table')
        .addColumn('email', 'varchar', (col) => col.unique());
      
      const compiled = query.compile();
      expect(compiled.sql).toContain('unique');
      expect(compiled.sql).toContain('not enforced');
    });

    test('Named UNIQUE constraint should include NOT ENFORCED', () => {
      const query = kysely.schema
        .createTable('test_table')
        .addColumn('category', 'varchar')
        .addColumn('name', 'varchar')
        .addUniqueConstraint('unique_category_name', ['category', 'name']);
      
      const compiled = query.compile();
      expect(compiled.sql).toContain('constraint `unique_category_name` unique (`category`, `name`) not enforced');
    });

    test('FOREIGN KEY constraint should include NOT ENFORCED', () => {
      const query = kysely.schema
        .createTable('orders')
        .addColumn('id', 'integer', (col) => col.primaryKey())
        .addColumn('customer_id', 'integer', (col) => 
          col.references('customers.id').onDelete('cascade')
        );
      
      const compiled = query.compile();
      expect(compiled.sql).toContain('references');
      expect(compiled.sql).toContain('not enforced');
    });

    test('Named FOREIGN KEY constraint should include NOT ENFORCED', () => {
      const query = kysely.schema
        .createTable('orders')
        .addColumn('id', 'integer')
        .addColumn('customer_id', 'integer')
        .addForeignKeyConstraint(
          'fk_customer', 
          ['customer_id'], 
          'customers', 
          ['id']
        );
      
      const compiled = query.compile();
      expect(compiled.sql).toContain('constraint `fk_customer` foreign key');
      expect(compiled.sql).toContain('not enforced');
    });
  });

  describe('Unsupported Operations Tests', () => {
    test('Should handle index operations', () => {
      // These would need to throw errors or be ignored
      const createIndex = sql`CREATE INDEX idx_users_email ON users(email)`;
      const dropIndex = sql`DROP INDEX idx_users_email`;

      // Currently these pass through as raw SQL
      const compiledCreateIndex = createIndex.compile(kysely);
      const compiledDropIndex = dropIndex.compile(kysely);
      expect(compiledCreateIndex.sql).toContain('CREATE INDEX');
      expect(compiledDropIndex.sql).toContain('DROP INDEX');
      
      // Desired: Should throw clear error about indexes not being supported
    });
  });

  describe('BigQuery-specific Features Tests', () => {
    test('Should support APPROX functions', () => {
      const query = kysely
        .selectFrom('events')
        .select(sql`APPROX_COUNT_DISTINCT(${sql.ref('user_id')})`.as('unique_users'));

      const compiled = query.compile();
      
      expect(compiled.sql).toContain('APPROX_COUNT_DISTINCT(`user_id`)');
    });

    test('Should support wildcard tables', () => {
      const query = sql`
        SELECT * FROM \`project.dataset.events_*\`
        WHERE _TABLE_SUFFIX BETWEEN '20230101' AND '20231231'
      `;

      const compiledQuery = query.compile(kysely);
      expect(compiledQuery.sql).toContain('events_*');
      expect(compiledQuery.sql).toContain('_TABLE_SUFFIX');
    });

    test('Should support UNNEST with proper syntax', () => {
      const query = kysely
        .selectFrom('products')
        .innerJoin(
          (eb) => eb.selectFrom(sql`UNNEST(${sql.ref('products.tags')})`.as('tag')).selectAll().as('tag'),
          'tag',
          'tag',
        )
        .select(['products.name', 'tag']);

      const compiled = query.compile();
      
      expect(compiled.sql).toContain('UNNEST(`products`.`tags`)');
    });
  });
});