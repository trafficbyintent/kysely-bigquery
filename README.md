# @trafficbyintent/kysely-bigquery

[Kysely](https://github.com/koskimas/kysely) adapter for [BigQuery](https://cloud.google.com/bigquery?hl=en).

## Installation

```bash
npm install @google-cloud/bigquery @trafficbyintent/kysely-bigquery
```

Or with yarn:
```bash
yarn add @google-cloud/bigquery @trafficbyintent/kysely-bigquery
```

This project was largely adapted from [kysely-planetscale](https://github.com/depot/kysely-planetscale) and forked from [@maktouch/kysely-bigquery](https://github.com/maktouch/kysely-bigquery).

## Requirements

- Node.js 18+ (tested with 18.x, 20.x, 22.x)
- BigQuery project with appropriate permissions

## Usage

Pass your BigQuery connection options, a BigQuery instance, a Dataset instance, or a Table instance into the dialect in
order to configure the Kysely client.
Follow [these docs](https://www.npmjs.com/package/@google-cloud/bigquery) for instructions on how to do so.

```typescript
import { Kysely } from 'kysely';
import { BigQueryDialect } from '@trafficbyintent/kysely-bigquery';

interface SomeTable {
  key: string;
  value: string;
}

interface Database {
  'some_dataset.some_table': SomeTable
}

// Let BigQueryDialect create the BigQuery instance:
const options: BigQueryOptions = ...;
const db = new Kysely<Database>({
  dialect: new BigQueryDialect({
    options,
    // Optional: prepend project ID to all table references
    defaultProject: 'my-gcp-project',
    // Optional: configure JSON columns for automatic serialization
    jsonColumns: {
      'some_dataset.some_table': ['metadata', 'settings']
    }
  })
});

// Or pass in an existing instance
const bigquery: BigQuery | Dataset | Table = ...;
const db = new Kysely<Database>({ dialect: new BigQueryDialect({ bigquery }) });
```

The dialect accepts either BigQuery connection options or an existing BigQuery/Dataset/Table instance. Authentication is handled by the `@google-cloud/bigquery` library itself. See their [documentation](https://www.npmjs.com/package/@google-cloud/bigquery) for authentication options.

For test environment setup, see [tests/README.md](tests/README.md).

### Key Features

- **Automatic null parameter handling** - The dialect automatically provides type hints for null parameters
- **JSON serialization/deserialization** - Registered JSON columns are automatically stringified on write and parsed on read
- **BigQuery SQL compatibility** - Automatic translation of MySQL-style queries to BigQuery syntax
- **Constraint support** - Handles BigQuery's unenforced constraints with proper `NOT ENFORCED` syntax

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

## JSON Data Handling

The dialect provides automatic JSON serialization for better developer experience when working with JSON data in BigQuery.

### Automatic JSON Serialization

When using STRING columns to store JSON data (the most common pattern), the dialect can automatically stringify JavaScript objects:

```typescript
// Configure JSON columns for automatic serialization
const db = new Kysely<Database>({
  dialect: new BigQueryDialect({
    bigquery: bigquery,
    jsonColumns: {
      'dataset.users': ['metadata', 'settings'],
      'dataset.products': ['specifications']
    }
  })
});

// Objects are automatically stringified for registered columns on write
await db
  .insertInto('dataset.users')
  .values({
    id: '123',
    name: 'John',
    metadata: { role: 'admin', permissions: ['read', 'write'] }, // Auto-stringified
    settings: { theme: 'dark', notifications: true }  // Auto-stringified
  })
  .execute();

// Registered columns are automatically parsed back to objects on read
const user = await db
  .selectFrom('dataset.users')
  .selectAll()
  .where('id', '=', '123')
  .executeTakeFirst();

console.log(user.metadata.role); // 'admin' - automatically parsed
```

### Manual JSON Handling

Without explicit configuration, you need to manually stringify JSON:

```typescript
await db
  .insertInto('dataset.users')
  .values({
    id: '123',
    metadata: JSON.stringify({ role: 'admin' }) // Manual stringify required
  })
  .execute();
```

### Native JSON Columns

For BigQuery's native JSON column type, you need to use `PARSE_JSON()`:

```typescript
// Native JSON columns require PARSE_JSON
await sql`
  INSERT INTO dataset.orders (id, data)
  VALUES (${orderId}, PARSE_JSON(${JSON.stringify(orderData)}))
`.execute(db);
```

### Querying JSON Data

Use BigQuery's JSON functions to query JSON data:

```typescript
const results = await sql`
  SELECT 
    JSON_VALUE(metadata, '$.role') as role,
    JSON_QUERY(settings, '$.features') as features
  FROM dataset.users
  WHERE JSON_VALUE(metadata, '$.role') = 'admin'
`.execute(db);
```

## Project-Qualified Table Names

BigQuery supports three-level table names: `project.dataset.table`. Since Kysely's parser only handles two-level names (`schema.table`), use the `defaultProject` config to automatically prepend your project ID:

```typescript
const db = new Kysely<Database>({
  dialect: new BigQueryDialect({
    bigquery: client,
    defaultProject: 'my-gcp-project',
  })
});

// Write queries with dataset.table — project is prepended automatically
db.selectFrom('analytics.events').selectAll();
// Generates: select * from `my-gcp-project`.`analytics`.`events`
```

Without `defaultProject`, two-level names work as expected:

```typescript
db.selectFrom('analytics.events').selectAll();
// Generates: select * from `analytics`.`events`
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

- Supports BigQuery's two-level `dataset.table` naming by default
- Use `defaultProject` config to enable three-level `project.dataset.table` references (project is prepended automatically)

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

### Important Notes

- **No Enforcement**: BigQuery constraints are metadata only and not enforced at runtime
- **Query Optimization**: Constraints help BigQuery's query optimizer improve performance
- **Foreign Key Restrictions**: Can only reference tables within the same dataset

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

### Core BigQuery Limitations

1. **No Transaction Support** - All operations are auto-committed

   ```typescript
   await db.transaction().execute(async (trx) => {
     // No-op - BigQuery doesn't support transactions.
     // Operations execute but are not wrapped in a transaction.
   });
   ```

2. **No Indexes** - BigQuery uses automatic optimization instead

3. **Case Sensitivity** - Table and column names are case-sensitive

4. **Query Size Limits** - Maximum result size is 10GB (use streaming for larger results)

### SQL Restrictions

- **UPDATE/DELETE** require WHERE clause (library automatically adds `WHERE TRUE` if missing)
- **INSERT** operations don't support `ON DUPLICATE KEY UPDATE`
- **Subqueries** have limited support for correlated subqueries in DML
- **Constraints** are metadata only and not enforced (see [BigQuery Constraints](#bigquery-constraints) section)

### Platform-Specific Limitations

These are BigQuery platform limitations that cannot be addressed by the dialect:

1. **Streaming Buffer Conflicts**
   - BigQuery doesn't allow UPDATE/DELETE operations on recently streamed data
   - Error: `UPDATE or DELETE statement over table would affect rows in the streaming buffer`
   - **Workaround**: Add delays between insert and update/delete operations, or use load jobs instead of streaming

   ```typescript
   // In tests or operations, add delays
   await db.insertInto('users').values(userData).execute();
   await new Promise(resolve => setTimeout(resolve, 2000)); // Wait for streaming buffer
   await db.updateTable('users').set(updates).where('id', '=', userId).execute();
   ```

2. **Eventual Consistency**
   - Table metadata and schema changes may not be immediately visible
   - INFORMATION_SCHEMA queries might not reflect recent changes
   - **Workaround**: Add delays or implement retry logic for metadata operations

3. **Complex Query Limitations**
   - Very complex WHERE conditions or joins may exceed BigQuery's query complexity limits
   - Some advanced SQL features may not be supported
   - **Workaround**: Simplify queries or use raw SQL for complex operations

4. **Rate Limits and Quotas**
   - BigQuery has various quotas for queries, DML statements, and API calls
   - Error: `Quota exceeded` or rate limit errors
   - **Workaround**: Implement exponential backoff and respect quota limits

5. **Data Type Restrictions**
   - ARRAY types cannot contain NULL values
   - STRUCT fields have naming restrictions
   - JSON columns (native) require specific syntax that differs from standard JSON operations

## Testing

### Running Tests Locally

```bash
# Run unit tests (no credentials required)
npm test

# Run unit tests with coverage
npm run test:coverage
```

### Integration Tests (Local Only)

Integration tests hit a real BigQuery instance and are **not** run in CI. They require a `.secrets` file with BigQuery credentials (see `.secrets.example`):

```bash
# Export credentials then run integration tests
export $(grep -v '^#' .secrets | xargs)
npm run test:integration

# Run all tests (unit + integration)
npm run test:all
```

### Testing GitHub Actions Locally

This project includes comprehensive GitHub Actions testing using [act](https://github.com/nektos/act):

```bash
# Install act (macOS)
brew install act

# Test all workflows
npm run test:github-actions

# Test specific workflow
./.github/test-actions.sh ci
```

#### Dry-Run Mode for Release Workflows

When testing release workflows locally, operations that would affect external services automatically run in safe mode:

- **NPM Publishing**: Uses `--dry-run` flag to simulate publishing without actually uploading
- **Git Operations**: Skipped with informative messages showing what would be pushed
- **GitHub Releases**: Mocked with detailed console output

This allows you to fully test release workflows without accidental deployments.

#### Apple Silicon Support

The test scripts automatically detect Apple Silicon (M1/M2) Macs and configure the appropriate container architecture.
