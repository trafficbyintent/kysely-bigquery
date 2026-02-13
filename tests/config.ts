import { BigQuery } from '@google-cloud/bigquery';
import { config } from 'dotenv';

/* Load configuration from .secrets file */
config({ path: '.secrets' });

/**
 * Get BigQuery client options from environment variables.
 */
export const getBigQueryOptions = () => {
  /* Local testing: credentials as a JSON blob via BIGQUERY_CREDENTIALS in .secrets */
  if (process.env.BIGQUERY_CREDENTIALS) {
    const credentials = JSON.parse(process.env.BIGQUERY_CREDENTIALS) as {
      client_email: string;
      private_key: string;
      project_id: string;
    };
    return {
      projectId: process.env.BIGQUERY_PROJECT_ID ?? credentials.project_id,
      credentials: {
        client_email: credentials.client_email,
        private_key: credentials.private_key,
      },
    };
  }

  /* Local: service account key file */
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    return {
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
      projectId: process.env.GCP_PROJECT_ID,
    };
  }

  /* Local: individual credential fields */
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
    throw new Error('No BigQuery credentials configured. Please check your .secrets file.');
  }
  return new BigQuery(options);
};