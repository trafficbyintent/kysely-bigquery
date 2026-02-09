import { type CompiledQuery } from 'kysely';

/* Type definitions for internal query nodes */
interface QueryNode {
  kind: string;
  into?: { table?: TableNode };
  table?: { table?: TableNode };
  columns?: ColumnNode[];
  updates?: UpdateNode[];
}

interface TableNode {
  kind: string;
  schema?: { name: string };
  identifier?: { name: string };
  name?: string;
}

interface ColumnNode {
  column?: { name: string };
  name?: string;
}

interface UpdateNode {
  column?: {
    column?: { name: string };
    name?: string;
  };
}

/**
 * Helper class to detect JSON columns in BigQuery tables
 * This is used to automatically serialize JSON data when inserting/updating
 */
export class JsonColumnDetector {
  readonly #jsonColumnCache = new Map<string, Set<string>>();

  /**
   * Register JSON columns for a table
   * @param tableName Full table name (e.g., 'dataset.table')
   * @param columns Array of column names that are JSON type
   */
  registerJsonColumns(tableName: string, columns: string[]): void {
    this.#jsonColumnCache.set(tableName, new Set(columns));
  }

  /**
   * Check if a column is a JSON column
   */
  isJsonColumn(tableName: string, columnName: string): boolean {
    const columns = this.#jsonColumnCache.get(tableName);
    return columns ? columns.has(columnName) : false;
  }

  /**
   * Returns a flat set of all registered JSON column names across all tables.
   * Used by the connection to determine which result columns to JSON-parse.
   *
   * Note: Because BigQuery results don't include table context, column names
   * are matched without table qualification. If two tables share a column name
   * and only one is registered as JSON, results from both tables will have
   * that column parsed. Avoid registering common column names (e.g., "data")
   * unless all tables with that column store JSON in it.
   */
  getRegisteredJsonColumnNames(): Set<string> {
    const allNames = new Set<string>();
    for (const columns of this.#jsonColumnCache.values()) {
      for (const col of columns) {
        allNames.add(col);
      }
    }
    return allNames;
  }

  /**
   * Extract table and column information from a compiled query
   */
  extractTableAndColumns(compiledQuery: CompiledQuery): {
    tableName?: string;
    columns?: string[];
    updateColumns?: string[];
  } {
    const query = compiledQuery.query as QueryNode | undefined;
    if (!query) {
      return {};
    }

    let tableName: string | undefined;
    let columns: string[] | undefined;
    let updateColumns: string[] | undefined;

    /* Handle INSERT queries */
    if (query.kind === 'InsertQueryNode' && query.into?.table) {
      tableName = this.extractTableName(query.into.table);
      if (query.columns) {
        columns = query.columns
          .map((col) => col.column?.name || col.name)
          .filter((name): name is string => Boolean(name));
      }
    }

    /* Handle UPDATE queries */
    if (query.kind === 'UpdateQueryNode' && query.table?.table) {
      tableName = this.extractTableName(query.table.table);
      if (query.updates) {
        updateColumns = query.updates
          .map((update) => update.column?.column?.name || update.column?.name)
          .filter((name): name is string => Boolean(name));
      }
    }

    const result: {
      tableName?: string;
      columns?: string[];
      updateColumns?: string[];
    } = {};

    if (tableName !== undefined) {
      result.tableName = tableName;
    }
    if (columns !== undefined) {
      result.columns = columns;
    }
    if (updateColumns !== undefined) {
      result.updateColumns = updateColumns;
    }

    return result;
  }

  /**
   * Extract table name from table node
   */
  private extractTableName(tableNode: TableNode | undefined): string | undefined {
    /* c8 ignore start */
    if (!tableNode) {
      return undefined;
    }
    /* c8 ignore stop */

    /* Handle SchemableIdentifierNode */
    if (tableNode.kind === 'SchemableIdentifierNode') {
      const schema = tableNode.schema?.name;
      const table = tableNode.identifier?.name;
      return schema && table ? `${schema}.${table}` : table;
    }

    /* Handle simple IdentifierNode */
    if (tableNode.kind === 'IdentifierNode') {
      return tableNode.name;
    }

    /* c8 ignore start - defensive fallback for unrecognized node types */
    return undefined;
    /* c8 ignore stop */
  }

  /**
   * Process parameters for JSON serialization based on the query.
   * @param compiledQuery - The compiled query containing column information
   * @param params - The query parameters to process
   * @returns Processed parameters with JSON objects stringified as needed
   */
  processParameters<T = unknown>(compiledQuery: CompiledQuery, params: readonly T[]): T[] {
    const { tableName, columns, updateColumns } = this.extractTableAndColumns(compiledQuery);

    if (!tableName) {
      return [...params] as T[];
    }

    const processedParams = [...params] as T[];

    /* For INSERT queries */
    if (columns && columns.length === params.length) {
      columns.forEach((col, index) => {
        if (this.shouldSerializeJson(tableName, col, params[index])) {
          processedParams[index] = JSON.stringify(params[index]) as T;
        }
      });
    }

    /* For UPDATE queries */
    if (updateColumns && compiledQuery.sql.toUpperCase().includes('UPDATE')) {
      /* Find parameter positions for update columns */
      let paramIndex = 0;
      const updatePattern = /SET\s+(.+?)\s+WHERE/i;
      const match = compiledQuery.sql.match(updatePattern);

      if (match) {
        updateColumns.forEach((col) => {
          if (
            paramIndex < params.length &&
            this.shouldSerializeJson(tableName, col, params[paramIndex])
          ) {
            processedParams[paramIndex] = JSON.stringify(params[paramIndex]) as T;
          }
          paramIndex++;
        });
      }
    }

    return processedParams;
  }

  /**
   * Determine if a value should be serialized as JSON
   */
  private shouldSerializeJson(tableName: string, columnName: string, value: unknown): boolean {
    /* Don't serialize null values */
    if (value === null || value === undefined) {
      return false;
    }

    /* Don't serialize non-objects */
    if (typeof value !== 'object') {
      return false;
    }

    /* Don't serialize Date or Buffer objects */
    if (value instanceof Date || value instanceof Buffer) {
      return false;
    }

    /* Check if column is registered as JSON */
    if (this.isJsonColumn(tableName, columnName)) {
      return true;
    }

    /*
     * Don't use naming convention for automatic serialization
     * Only serialize if explicitly registered to avoid breaking STRUCT columns
     * Users can register columns if they want automatic serialization
     */

    return false;
  }
}
