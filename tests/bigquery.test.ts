import {expect, test, vi} from 'vitest';
import {BigQueryDialect} from '../src';
import {Kysely, sql} from 'kysely';
import {expectedSimpleSelectCompiled} from './helpers';

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
    .where(eb => eb.or([
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
    .where('amount', 'between', [100, 500])
    .selectAll();

  const betweenCompiled = betweenQuery.compile();
  expect(betweenCompiled.sql).toBe('select * from `transactions` where `amount` between (?, ?)');
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
    .where('customer_id', 'in', eb =>
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
      eb => eb.selectFrom('orders')
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
    'select `name`, `email` from `customers` where `status` = ? union select `company_name` as `name`, `contact_email` as `email` from `vendors` where `active` = ?'
  );
  expect(compiled.parameters).toEqual(['active', true]);
});

test('CTEs (Common Table Expressions)', () => {
  const kysely = new Kysely<any>({
    dialect: new BigQueryDialect(),
  });

  const query = kysely
    .with('high_value_customers', db =>
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
    .innerJoin(sql`UNNEST(${sql.ref('products.tags')})`.as('tag'), sql`true`, sql`true`)
    .select(['products.name', 'tag'])
    .where('tag', '=', 'electronics');

  const unnestCompiled = unnestQuery.compile();
  expect(unnestCompiled.sql).toBe(
    'select `products`.`name`, `tag` from `products` inner join UNNEST(`products`.`tags`) as `tag` on true = true where `tag` = ?'
  );
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