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

   Copy `.env.example` to `.env` and configure your BigQuery credentials:

   ```bash
   cp .env.example .env
   ```

   Edit `.env` with your credentials. You can use either:

   - A service account key file (recommended)
   - Individual credentials (client email and private key)

3. **Set up BigQuery Test Environment**

   Run the setup script to create the necessary datasets and tables:

   ```bash
   # Load environment variables and run setup
   source .env && ./scripts/setup-bigquery-test.sh
   ```

   This script creates:

   - `features` dataset with a `metadata` table
   - `api` dataset with a `bank_account_transactions` table
   - Sample test data

4. **Run Integration Tests**

   ```bash
   npm run test:integration
   ```

## Test Structure

- `tests/*.test.ts` - Unit tests with mocked BigQuery client
- `tests/*.integration.test.ts` - Integration tests requiring BigQuery connection