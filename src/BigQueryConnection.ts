import {
  CompiledQuery,
  DatabaseConnection,
  QueryResult,
} from 'kysely';

import { BigQuery, Dataset, Table } from '@google-cloud/bigquery';
import { BigQueryDialectConfig } from '.';
import { JsonColumnDetector } from './jsonColumnDetector';


/**
 * BigQuery database connection implementation for Kysely.
 * 
 * Handles query execution and streaming for BigQuery.
 */
export class BigQueryConnection implements DatabaseConnection {
  #client: BigQuery | Dataset | Table;
  #jsonDetector: JsonColumnDetector;

  constructor(config: BigQueryDialectConfig) {
    this.#client = config.bigquery ?? new BigQuery(config.options);
    this.#jsonDetector = new JsonColumnDetector();
    
    /* Register known JSON columns if provided in config */
    if (config.jsonColumns) {
      for (const [tableName, columns] of Object.entries(config.jsonColumns)) {
        this.#jsonDetector.registerJsonColumns(tableName, columns);
      }
    }
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    try {
      const params = [...compiledQuery.parameters];
      const nullParamIndices: number[] = [];
      
      /* Process parameters to handle nulls and JSON serialization */
      let processedParams = this.#jsonDetector.processParameters(compiledQuery, params);
      
      /* Check for null parameters */
      processedParams = processedParams.map((param, index) => {
        if (param === null) {
          nullParamIndices.push(index);
          return null;
        }
        return param;
      });

      const options: any = {
        query: compiledQuery.sql,
        params: processedParams,
      };
      
      /* BigQuery needs types array for ALL parameters when there are null parameters */
      if (nullParamIndices.length > 0) {
        options.types = params.map((param) => {
          if (param === null) {
            return 'STRING';
          } else if (typeof param === 'number') {
            return Number.isInteger(param) ? 'INT64' : 'FLOAT64';
          } else if (typeof param === 'boolean') {
            return 'BOOL';
          } else if (param instanceof Date) {
            return 'TIMESTAMP';
          } else if (param instanceof Buffer) {
            return 'BYTES';
          } else if (typeof param === 'object') {
            /* Let BigQuery infer the type for arrays and objects
             * They could be ARRAY, STRUCT, or JSON depending on the column */
            return 'JSON';
          } else {
            return 'STRING';
          }
        });
      }

      const [rows] = await this.#client.query(options);

      /* Process result rows to parse JSON strings back to objects */
      const processedRows = Array.isArray(rows) ? rows.map(row => {
        const processedRow: any = {};
        for (const [key, value] of Object.entries(row)) {
          if (typeof value === 'string' && value.length > 0) {
            /* Try to parse JSON strings */
            try {
              const trimmed = value.trim();
              if ((trimmed.startsWith('{') && trimmed.endsWith('}')) ||
                  (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
                processedRow[key] = JSON.parse(value);
              } else {
                processedRow[key] = value;
              }
            } catch {
              /* If parsing fails, keep the original string */
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
        numAffectedRows: undefined as any,
        /**
         * @deprecated numUpdatedOrDeletedRows is deprecated in kysely >= 0.23.
         * Kept for backward compatibility.
         */
        numUpdatedOrDeletedRows: undefined as any,
      };
    } catch (error) {
      /* Provide more helpful error messages */
      if (error instanceof Error) {
        if (error.message.includes('Parameter types must be provided for null values') ||
            error.message.includes('Incorrect number of parameter types provided')) {
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

  async *streamQuery<O>(compiledQuery: CompiledQuery, _chunkSize: number): AsyncIterableIterator<QueryResult<O>> {
    const params = [...compiledQuery.parameters];
    const nullParamIndices: number[] = [];
    
    /* Process parameters to handle nulls and JSON serialization */
    let processedParams = this.#jsonDetector.processParameters(compiledQuery, params);
    
    /* Check for null parameters */
    processedParams = processedParams.map((param, index) => {
      if (param === null) {
        nullParamIndices.push(index);
        return null;
      }
      return param;
    });

    const options: any = {
      query: compiledQuery.sql,
      params: processedParams,
    };
    
    /* BigQuery needs types array for ALL parameters when there are null parameters */
    if (nullParamIndices.length > 0) {
      options.types = params.map((param) => {
        if (param === null) {
          return 'STRING';
        } else if (typeof param === 'number') {
          return Number.isInteger(param) ? 'INT64' : 'FLOAT64';
        } else if (typeof param === 'boolean') {
          return 'BOOL';
        } else if (param instanceof Date) {
          return 'TIMESTAMP';
        } else if (param instanceof Buffer) {
          return 'BYTES';
        } else if (typeof param === 'object') {
          return 'JSON';
        } else {
          return 'STRING';
        }
      });
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
        /* Process row to parse JSON strings */
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
      /* Handle stream errors */
      if (error instanceof Error) {
        throw new Error(`BigQuery stream error: ${error.message}`);
      }
      throw error;
    }
  }
}