import { BigQuery } from '@google-cloud/bigquery';
import { config } from 'dotenv';

config();

/**
 * Get BigQuery client options from environment variables.
 */
export const getBigQueryOptions = () => {
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    return {
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
      projectId: process.env.GCP_PROJECT_ID,
    };
  }

  if (process.env.GCP_CLIENT_EMAIL && process.env.GCP_PRIVATE_KEY) {
    return {
      projectId: process.env.GCP_PROJECT_ID,
      credentials: {
        client_email: process.env.GCP_CLIENT_EMAIL,
        private_key: process.env.GCP_PRIVATE_KEY.replace(/\\n/g, '\n'),
      },
    };
  }

  return undefined;
};

/**
 * Create a BigQuery client instance with configured credentials.
 */
export const createBigQueryInstance = () => {
  const options = getBigQueryOptions();
  if (!options) {
    throw new Error('No BigQuery credentials configured. Please check your .env file.');
  }
  return new BigQuery(options);
};