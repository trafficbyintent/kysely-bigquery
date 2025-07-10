import { CompiledQuery } from 'kysely';

/**
 * Helper class to detect JSON columns in BigQuery tables
 * This is used to automatically serialize JSON data when inserting/updating
 */
export class JsonColumnDetector {
  private jsonColumnCache: Map<string, Set<string>> = new Map();

  /**
   * Register JSON columns for a table
   * @param tableName Full table name (e.g., 'dataset.table')
   * @param columns Array of column names that are JSON type
   */
  registerJsonColumns(tableName: string, columns: string[]): void {
    this.jsonColumnCache.set(tableName, new Set(columns));
  }

  /**
   * Check if a column is a JSON column
   */
  isJsonColumn(tableName: string, columnName: string): boolean {
    const columns = this.jsonColumnCache.get(tableName);
    return columns ? columns.has(columnName) : false;
  }

  /**
   * Extract table and column information from a compiled query
   */
  extractTableAndColumns(compiledQuery: CompiledQuery): {
    tableName?: string;
    columns?: string[];
    updateColumns?: string[];
  } {
    const query = compiledQuery.query as any;
    if (!query) return {};

    let tableName: string | undefined;
    let columns: string[] | undefined;
    let updateColumns: string[] | undefined;

    // Handle INSERT queries
    if (query.kind === 'InsertQueryNode' && query.into?.table) {
      tableName = this.extractTableName(query.into.table);
      if (query.columns) {
        columns = query.columns.map((col: any) => 
          col.column?.name || col.name
        ).filter(Boolean);
      }
    }

    // Handle UPDATE queries
    if (query.kind === 'UpdateQueryNode' && query.table?.table) {
      tableName = this.extractTableName(query.table.table);
      if (query.updates) {
        updateColumns = query.updates.map((update: any) => 
          update.column?.column?.name || update.column?.name
        ).filter(Boolean);
      }
    }

    return { tableName, columns, updateColumns };
  }

  /**
   * Extract table name from table node
   */
  private extractTableName(tableNode: any): string | undefined {
    if (!tableNode) return undefined;

    // Handle SchemableIdentifierNode
    if (tableNode.kind === 'SchemableIdentifierNode') {
      const schema = tableNode.schema?.name;
      const table = tableNode.identifier?.name;
      return schema && table ? `${schema}.${table}` : table;
    }

    // Handle simple IdentifierNode
    if (tableNode.kind === 'IdentifierNode') {
      return tableNode.name;
    }

    return undefined;
  }

  /**
   * Detect if a column name is likely to be a JSON column based on naming conventions
   * This is a fallback when we don't have schema information
   */
  isLikelyJsonColumn(columnName: string): boolean {
    const jsonColumnPatterns = [
      'metadata',
      'settings',
      'config',
      'configuration',
      'preferences',
      'options',
      'data',
      'json',
      'payload',
      'body',
      'content',
      'attributes',
      'properties',
      'params',
      'extra',
      'custom'
    ];

    const lowerColumnName = columnName.toLowerCase();
    return jsonColumnPatterns.some(pattern => 
      lowerColumnName === pattern || 
      lowerColumnName.endsWith(`_${pattern}`) ||
      lowerColumnName.startsWith(`${pattern}_`)
    );
  }

  /**
   * Process parameters for JSON serialization based on the query
   */
  processParameters(compiledQuery: CompiledQuery, params: readonly any[]): any[] {
    const { tableName, columns, updateColumns } = this.extractTableAndColumns(compiledQuery);
    
    if (!tableName) return [...params];

    const processedParams = [...params];
    
    // For INSERT queries
    if (columns && columns.length === params.length) {
      columns.forEach((col, index) => {
        if (this.shouldSerializeJson(tableName, col, params[index])) {
          processedParams[index] = JSON.stringify(params[index]);
        }
      });
    }

    // For UPDATE queries
    if (updateColumns && compiledQuery.sql.toUpperCase().includes('UPDATE')) {
      // Find parameter positions for update columns
      let paramIndex = 0;
      const updatePattern = /SET\s+(.+?)\s+WHERE/i;
      const match = compiledQuery.sql.match(updatePattern);
      
      if (match) {
        updateColumns.forEach((col) => {
          if (paramIndex < params.length && 
              this.shouldSerializeJson(tableName, col, params[paramIndex])) {
            processedParams[paramIndex] = JSON.stringify(params[paramIndex]);
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
  private shouldSerializeJson(tableName: string, columnName: string, value: any): boolean {
    // Don't serialize null values
    if (value === null || value === undefined) return false;

    // Don't serialize non-objects
    if (typeof value !== 'object') return false;

    // Don't serialize Date or Buffer objects
    if (value instanceof Date || value instanceof Buffer) return false;

    // Check if column is registered as JSON
    if (this.isJsonColumn(tableName, columnName)) return true;

    // Don't use naming convention for automatic serialization
    // Only serialize if explicitly registered to avoid breaking STRUCT columns
    // Users can register columns if they want automatic serialization

    return false;
  }
}