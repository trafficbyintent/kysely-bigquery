# @maktouch/kysely-bigquery

[Kysely](https://github.com/koskimas/kysely) adapter for [BigQuery](https://cloud.google.com/bigquery?hl=en).

```bash
npm i @google-cloud/bigquery @maktouch/kysely-bigquery
```

This project was largely adapted from [kysely-planetscale](https://github.com/depot/kysely-planetscale).

## Usage

Pass your BigQuery connection options, a BigQuery instance, a Dataset instance, or a Table instance into the dialect in
order to configure the Kysely client.
Follow [these docs](https://www.npmjs.com/package/@google-cloud/bigquery) for instructions on how to do so.

```typescript
import { Kysely } from 'kysely';
import { BigQueryDialect } from '@maktouch/kysely-bigquery';

interface SomeTable {
  key: string;
  value: string;
}

interface Database {
  'some_dataset.some_table': SomeTable
}

// Let BigQueryDialect create the BiqQuery instance:
const options: BigQueryOptions = ...;
const db = new Kysely<Database>({ dialect: new BigQueryDialect({ options }) });

// Or pass in an existing instance
const bigquery: BigQuery | Dataset | Table = ...;
const db = new Kysely<Database>({ dialect: new BigQueryDialect({ bigquery }) });
```

The dialect accepts either BigQuery connection options or an existing BigQuery/Dataset/Table instance. Authentication is handled by the `@google-cloud/bigquery` library itself. See their [documentation](https://www.npmjs.com/package/@google-cloud/bigquery) for authentication options.

For test environment setup, see [tests/README.md](tests/README.md).

## Data Type Mapping

BigQuery data types are mapped to TypeScript types as follows:

| BigQuery Type | TypeScript Type  | Notes                             |
| ------------- | ---------------- | --------------------------------- |
| INT64         | number or string | Large values returned as strings  |
| FLOAT64       | number           |                                   |
| NUMERIC       | string           | Preserved precision               |
| BIGNUMERIC    | string           | Preserved precision               |
| STRING        | string           |                                   |
| BYTES         | Buffer           | Use `FROM_BASE64()` for insertion |
| BOOL          | boolean          |                                   |
| DATE          | string           | Format: 'YYYY-MM-DD'              |
| DATETIME      | string           | Format: 'YYYY-MM-DD HH:MM:SS'     |
| TIMESTAMP     | Date             | JavaScript Date object            |
| TIME          | string           | Format: 'HH:MM:SS'                |
| JSON          | any              | Use JSON literals for insertion   |
| ARRAY<T>      | T[]              |                                   |
| STRUCT<...>   | object           | Nested object structure           |

### Special Type Handling Examples

```typescript
// INT64
await db
  .insertInto("users")
  .values({
    id: 12345,
    big_id: "9223372036854775807",
  })
  .execute();

// BYTES
await sql`
  INSERT INTO files (content) 
  VALUES (FROM_BASE64(${Buffer.from("Hello").toString("base64")}))
`.execute(db);

// JSON
await sql`
  INSERT INTO logs (data) 
  VALUES (JSON '{"level": "info", "message": "test"}')
`.execute(db);

// ARRAY
await db
  .insertInto("products")
  .values({
    tags: ["electronics", "laptop", "computer"],
  })
  .execute();
```

## BigQuery SQL Compatibility

The `BigQueryCompiler` extends Kysely's MySQL query compiler to handle BigQuery-specific SQL syntax differences. It automatically translates common MySQL patterns to their BigQuery equivalents, allowing you to write more portable code.

### Automatic SQL Translations

#### Set Operations

- `UNION` → `UNION DISTINCT` (BigQuery requires explicit DISTINCT)

#### Function Translations

- `NOW()` → `CURRENT_TIMESTAMP()`
- `LENGTH()` → `CHAR_LENGTH()` (for character count instead of byte count)
- `DATE_FORMAT(date, format)` → `FORMAT_TIMESTAMP(format, date)` (parameter order is swapped)

#### DML Requirements

- **UPDATE without WHERE**: Automatically adds `WHERE TRUE` (BigQuery requires a WHERE clause)
- **DELETE without WHERE**: Automatically adds `WHERE TRUE` (BigQuery requires a WHERE clause)

#### Table Naming

- Supports BigQuery's `project.dataset.table` naming convention
- Automatically handles dot-separated schema names

### Example Translations

```typescript
await db
  .selectFrom("users")
  .select(sql`NOW()`.as("current_time"))
  .where("name", "like", sql`CONCAT('%', ${search}, '%')`)
  .union(db.selectFrom("archived_users").select(sql`NOW()`.as("current_time")))
  .execute();

await db.updateTable("users").set({ status: "active" }).execute();
```

### Raw SQL Support

The compiler also translates functions within raw SQL strings:

```typescript
await sql`SELECT DATE_FORMAT(created_at, '%Y-%m-%d') as date FROM users`.execute(
  db
);
// Generates: SELECT FORMAT_TIMESTAMP('%Y-%m-%d', created_at) as date FROM users
```

## BigQuery Constraints

BigQuery supports constraint syntax (PRIMARY KEY, FOREIGN KEY, UNIQUE) but these constraints are **not enforced** at runtime. They serve as metadata for query optimization and documentation purposes.

When using Kysely with the BigQuery dialect, all constraints automatically include the `NOT ENFORCED` qualifier as required by BigQuery.

### Constraint Examples

#### Primary Key

```typescript
await db.schema
  .createTable("users")
  .addColumn("id", "integer", (col) => col.primaryKey())
  .execute();

await db.schema
  .createTable("order_items")
  .addColumn("order_id", "integer")
  .addColumn("product_id", "integer")
  .addPrimaryKeyConstraint("pk_order_items", ["order_id", "product_id"])
  .execute();
```

#### Unique Constraint

```typescript
await db.schema
  .createTable("users")
  .addColumn("email", "varchar", (col) => col.unique())
  .execute();

await db.schema
  .createTable("products")
  .addColumn("category", "varchar")
  .addColumn("name", "varchar")
  .addUniqueConstraint("unique_category_name", ["category", "name"])
  .execute();
```

#### Foreign Key

```typescript
await db.schema
  .createTable("orders")
  .addColumn("customer_id", "integer", (col) =>
    col.references("customers.id").onDelete("cascade")
  )
  .execute();

await db.schema
  .createTable("orders")
  .addColumn("customer_id", "integer")
  .addForeignKeyConstraint("fk_customer", ["customer_id"], "customers", ["id"])
  .execute();
```

### Important Limitations

1. **No Enforcement**: BigQuery will not validate constraints during data insertion. You can:

   - Insert duplicate values in PRIMARY KEY columns
   - Insert duplicate values in UNIQUE columns
   - Insert NULL values in PRIMARY KEY columns
   - Insert foreign key values that don't exist in the referenced table

2. **Foreign Key Restrictions**:

   - Foreign keys can only reference tables within the same dataset
   - The referenced table must exist at the time of table creation

3. **Query Optimization**: Despite being unenforced, these constraints help BigQuery's query optimizer with:
   - Inner Join Elimination
   - Outer Join Elimination
   - Join Reordering

### Complete Table Example

```typescript
await db.schema
  .createTable("orders")
  .addColumn("id", "integer", (col) => col.primaryKey())
  .addColumn("order_number", "varchar", (col) => col.unique().notNull())
  .addColumn("customer_id", "integer", (col) =>
    col.references("customers.id").notNull()
  )
  .addColumn("total_amount", "decimal", (col) => col.notNull())
  .addColumn("created_at", "timestamp", (col) =>
    col.defaultTo(sql`CURRENT_TIMESTAMP`).notNull()
  )
  .execute();
```

This generates:

```sql
CREATE TABLE `orders` (
  `id` integer PRIMARY KEY NOT ENFORCED,
  `order_number` varchar UNIQUE NOT ENFORCED NOT NULL,
  `customer_id` integer REFERENCES `customers` (`id`) NOT ENFORCED NOT NULL,
  `total_amount` decimal NOT NULL,
  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
)
```

### Raw SQL Constraints

When using raw SQL, you must manually add the `NOT ENFORCED` qualifier:

```typescript
await sql`
  CREATE TABLE users (
    id INT64 NOT NULL,
    email STRING,
    CONSTRAINT pk_users PRIMARY KEY (id) NOT ENFORCED,
    CONSTRAINT unique_email UNIQUE (email) NOT ENFORCED
  )
`.execute(db);
```

Without `NOT ENFORCED`, BigQuery will reject the constraint definition.

## Limitations

### No Transaction Support

BigQuery doesn't support transactions. All operations are auto-committed:

```typescript
await db.transaction().execute(async (trx) => {
  // Throws error - BigQuery doesn't support transactions
});
```

### Unenforced Constraints

While BigQuery accepts constraint syntax, it doesn't enforce them:

- PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints are metadata only
- Used for query optimization, not data validation
- Your application must ensure data integrity

### DML Restrictions

- UPDATE and DELETE require WHERE clause (library adds `WHERE TRUE` if missing)
- No support for INSERT ... ON DUPLICATE KEY UPDATE
- Limited support for correlated subqueries in DML

### Other Limitations

- Foreign keys can only reference tables in the same dataset
- No indexes (BigQuery uses automatic optimization)
- Case-sensitive table and column names
- Maximum query result size: 10GB (use streaming for larger results)

## API Reference

### Exported Classes

- **BigQueryDialect**: Main dialect implementation for Kysely
- **BigQueryAdapter**: Adapter for BigQuery-specific SQL generation
- **BigQueryDriver**: Database driver handling connections
- **BigQueryConnection**: Individual connection management
- **BigQueryIntrospector**: Schema introspection utilities
- **BigQueryCompiler**: SQL query compiler with BigQuery translations

### Configuration Types

- **BigQueryDialectConfig**: Configuration options for the dialect
- **BigQueryDialectConfigOptions**: BigQuery client options

For detailed API documentation, refer to the TypeScript definitions in the source code.
