# Testing

This package includes both unit tests and integration tests. Unit tests use mocks and can run without BigQuery access, while integration tests require a real BigQuery connection.

## Running Tests

```bash
# Run unit tests only (default)
npm test

# Watch mode for unit tests
npm run test:watch

# Run integration tests
npm run test:integration

# Run all tests
npm run test:all
```

## Setting up Integration Tests

Integration tests require access to a BigQuery instance. Follow these steps to set up your test environment:

1. **Install Google Cloud SDK**

   ```bash
   # macOS
   brew install google-cloud-sdk

   # Other platforms
   # Visit: https://cloud.google.com/sdk/docs/install
   ```

2. **Configure Authentication**

   Copy `.secrets.example` to `.secrets` and configure your BigQuery credentials:

   ```bash
   cp .secrets.example .secrets
   ```

   Edit `.secrets` with your credentials:

   ```bash
   # Service account key file (recommended)
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
   GCP_PROJECT_ID=your-project-id
   
   # Optional: Specify dataset for tests
   BIGQUERY_DATASET=test_dataset
   ```

3. **Set up BigQuery Test Environment**

   Run the setup script to create the necessary datasets and tables:

   ```bash
   # Load environment variables and run setup
   source .secrets && ./scripts/setup-bigquery-test.sh
   ```

   This script creates:

   - `features` dataset with a `metadata` table
   - `api` dataset with a `bank_account_transactions` table
   - Sample test data
   
   Note: Integration tests also create temporary tables in `test_dataset` which are cleaned up automatically.

4. **Run Integration Tests**

   ```bash
   npm run test:integration
   ```

## Test Structure

- `bigquery.test.ts` - Unit tests with mocked BigQuery client (includes constraint tests)
- `bigquery.integration.test.ts` - Integration tests requiring BigQuery connection (includes MySQL vs BigQuery differences)
- `config.ts` - Shared test configuration
- `helpers.ts` - Test utilities and fixtures

## Troubleshooting

### Common Test Failures

1. **"Table not found" errors**
   - Ensure your service account has BigQuery Admin permissions
   - Run the setup script to create required datasets
   - Check that `GCP_PROJECT_ID` matches your actual project

2. **Authentication errors**
   - Verify `GOOGLE_APPLICATION_CREDENTIALS` points to a valid service account key
   - Ensure the service account has necessary BigQuery permissions

3. **Timeout errors**
   - Integration tests have 10-second timeouts for most operations
   - Network latency to BigQuery can cause timeouts
   - Consider increasing timeout values for slow connections

4. **Data type errors**
   - BigQuery is strict about data types
   - Use proper casting for INT64, NUMERIC types
   - Use `FROM_BASE64()` for BYTES insertion
   - Use JSON literals for JSON fields