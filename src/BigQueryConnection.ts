import {
  CompiledQuery,
  DatabaseConnection,
  QueryResult,
} from 'kysely';

import { BigQuery, Dataset, Table } from '@google-cloud/bigquery';
import { BigQueryDialectConfig } from '.';


/**
 * BigQuery database connection implementation for Kysely.
 * 
 * Handles query execution and streaming for BigQuery.
 */
export class BigQueryConnection implements DatabaseConnection {
  #client: BigQuery | Dataset | Table;

  constructor(config: BigQueryDialectConfig) {
    this.#client = config.bigquery ?? new BigQuery(config.options);
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    try {
      const params = [...compiledQuery.parameters];
      const nullParamIndices: number[] = [];
      const types: string[] = [];
      
      // Process parameters to handle nulls and JSON
      const processedParams = params.map((param, index) => {
        if (param === null) {
          // Track null parameter positions
          nullParamIndices.push(index);
          types.push('STRING'); // Default type for nulls
          return null;
        } else if (param !== undefined && typeof param === 'object' && 
                   !(param instanceof Date) && !(param instanceof Buffer)) {
          // Serialize JSON objects to strings for BigQuery
          return JSON.stringify(param);
        }
        return param;
      });

      const options: any = {
        query: compiledQuery.sql,
        params: processedParams,
      };
      
      // BigQuery needs types array when there are null parameters
      // According to BigQuery docs, we only provide types for the null parameters
      if (nullParamIndices.length > 0) {
        options.types = types;
      }

      const [rows] = await this.#client.query(options);

      // Process result rows to parse JSON strings back to objects
      const processedRows = Array.isArray(rows) ? rows.map(row => {
        const processedRow: any = {};
        for (const [key, value] of Object.entries(row)) {
          if (typeof value === 'string' && value.length > 0) {
            // Try to parse JSON strings
            try {
              const trimmed = value.trim();
              if ((trimmed.startsWith('{') && trimmed.endsWith('}')) ||
                  (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
                processedRow[key] = JSON.parse(value);
              } else {
                processedRow[key] = value;
              }
            } catch {
              // If parsing fails, keep the original string
              processedRow[key] = value;
            }
          } else {
            processedRow[key] = value;
          }
        }
        return processedRow;
      }) : [];

      return {
        insertId: undefined,
        rows: processedRows as O[],
        numAffectedRows: undefined,
        /**
         * @deprecated numUpdatedOrDeletedRows is deprecated in kysely >= 0.23.
         * Kept for backward compatibility.
         */
        numUpdatedOrDeletedRows: undefined,
      };
    } catch (error) {
      // Provide more helpful error messages
      if (error instanceof Error) {
        if (error.message.includes('Parameter types must be provided for null values')) {
          throw new Error(
            `BigQuery query failed: ${error.message}\n` +
            `Hint: The BigQuery dialect now automatically handles null parameters. ` +
            `If you're still seeing this error, please report it as a bug.`
          );
        }
        throw new Error(`BigQuery query failed: ${error.message}`);
      }
      throw error;
    }
  }

  async beginTransaction() {
    throw new Error('Transactions are not supported.');
  }

  async commitTransaction() {
    throw new Error('Transactions are not supported.');
  }

  async rollbackTransaction() {
    throw new Error('Transactions are not supported.');
  }

  async *streamQuery<O>(compiledQuery: CompiledQuery, chunkSize: number): AsyncIterableIterator<QueryResult<O>> {
    const params = [...compiledQuery.parameters];
    const nullParamIndices: number[] = [];
    const types: string[] = [];
    
    // Process parameters to handle nulls and JSON (same as executeQuery)
    const processedParams = params.map((param, index) => {
      if (param === null) {
        nullParamIndices.push(index);
        types.push('STRING');
        return null;
      } else if (param !== undefined && typeof param === 'object' && 
                 !(param instanceof Date) && !(param instanceof Buffer)) {
        return JSON.stringify(param);
      }
      return param;
    });

    const options: any = {
      query: compiledQuery.sql,
      params: processedParams,
    };
    
    if (nullParamIndices.length > 0) {
      options.types = types;
    }

    let stream;
    try {
      stream = await this.#client.createQueryStream(options);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`BigQuery stream query failed: ${error.message}`);
      }
      throw error;
    }

    try {
      for await (const row of stream) {
        // Process row to parse JSON strings
        const processedRow: any = {};
        for (const [key, value] of Object.entries(row)) {
          if (typeof value === 'string' && value.length > 0) {
            try {
              const trimmed = value.trim();
              if ((trimmed.startsWith('{') && trimmed.endsWith('}')) ||
                  (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
                processedRow[key] = JSON.parse(value);
              } else {
                processedRow[key] = value;
              }
            } catch {
              processedRow[key] = value;
            }
          } else {
            processedRow[key] = value;
          }
        }
        
        yield {
          rows: [processedRow],
        };
      }
    } catch (error) {
      // Handle stream errors
      if (error instanceof Error) {
        throw new Error(`BigQuery stream error: ${error.message}`);
      }
      throw error;
    }
  }
}