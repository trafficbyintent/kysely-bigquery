#!/bin/bash

# Setup script for BigQuery test environment
# This script creates the necessary datasets and tables for integration tests

PROJECT_ID=${GCP_PROJECT_ID}
LOCATION=${BIGQUERY_LOCATION:-US}

if [ -z "$PROJECT_ID" ]; then
    echo "Error: GCP_PROJECT_ID environment variable is not set"
    echo "Please set it in your .env file or export it before running this script"
    exit 1
fi

echo "Setting up BigQuery test environment for project: $PROJECT_ID"

# Create features dataset
echo "Creating features dataset..."
bq mk --dataset --location=$LOCATION $PROJECT_ID:features 2>/dev/null || echo "Dataset features already exists"

# Create metadata table
echo "Creating metadata table..."
bq mk --table $PROJECT_ID:features.metadata \
id:INTEGER,agg_universe:STRING,category:STRING,compounding:STRING,created_at:TIMESTAMP,description:STRING,interval_measured_quantity:INTEGER,interval_measured_units:STRING,name:STRING,name_raw:STRING,public:INTEGER,subcategory:STRING,type:STRING,updated_at:TIMESTAMP,inserted_at:TIMESTAMP 2>/dev/null || echo "Table features.metadata already exists"

# Create api dataset
echo "Creating api dataset..."
bq mk --dataset --location=$LOCATION $PROJECT_ID:api 2>/dev/null || echo "Dataset api already exists"

# Create bank_account_transactions table with schema
echo "Creating bank_account_transactions table..."
cat > /tmp/bank_transactions_schema.json << 'EOF'
[
  {"name": "id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "bank_account_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "remote_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "fpath_account_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "date", "type": "DATE", "mode": "NULLABLE"},
  {"name": "original_description", "type": "STRING", "mode": "NULLABLE"},
  {"name": "pending", "type": "INT64", "mode": "NULLABLE"},
  {"name": "processing_status", "type": "STRING", "mode": "NULLABLE"},
  {"name": "amount", "type": "INT64", "mode": "NULLABLE"},
  {"name": "iso_currency_code", "type": "STRING", "mode": "NULLABLE"},
  {"name": "unofficial_currency_code", "type": "STRING", "mode": "NULLABLE"},
  {"name": "bank_account_category_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "authorized_date", "type": "DATE", "mode": "NULLABLE"},
  {"name": "name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "merchant_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "pending_transaction_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "transaction_type", "type": "STRING", "mode": "NULLABLE"},
  {"name": "account_owner", "type": "STRING", "mode": "NULLABLE"},
  {"name": "amount_usd", "type": "INT64", "mode": "NULLABLE"},
  {"name": "transaction_category", "type": "STRING", "mode": "NULLABLE"},
  {"name": "transaction_status", "type": "STRING", "mode": "NULLABLE"},
  {"name": "counter_party", "type": "STRUCT", "mode": "NULLABLE", "fields": [
    {"name": "type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"}
  ]},
  {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "ntropy_batch_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "enriched_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "enriched_by", "type": "STRING", "mode": "NULLABLE"},
  {"name": "enrichment_cat1", "type": "STRING", "mode": "NULLABLE"},
  {"name": "enrichment_cat2", "type": "STRING", "mode": "NULLABLE"},
  {"name": "enrichment_cat3", "type": "STRING", "mode": "NULLABLE"},
  {"name": "enrichment_cat4", "type": "STRING", "mode": "NULLABLE"},
  {"name": "enrichment_merchant_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "inserted_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
]
EOF

bq mk --table --schema /tmp/bank_transactions_schema.json $PROJECT_ID:api.bank_account_transactions 2>/dev/null || echo "Table api.bank_account_transactions already exists"

# Clean up temp file
rm -f /tmp/bank_transactions_schema.json

# Insert test data
echo "Inserting test data..."
bq query --use_legacy_sql=false "INSERT INTO \`$PROJECT_ID.features.metadata\` (id, agg_universe, category, compounding, created_at, description, interval_measured_quantity, interval_measured_units, name, name_raw, public, subcategory, type, updated_at, inserted_at) VALUES (28, 'n.a.', 'cac', 'n.a.', TIMESTAMP('2022-06-24T06:28:56.000Z'), 'Monthly customer acquisition cost (CAC). Marketing costs only.', 1, 'month', 'cac', 'cac_marketing', 0, 'amount', 'feature', TIMESTAMP('2023-11-13T23:10:54.000Z'), TIMESTAMP('2023-11-14T00:01:53.000Z'))" 2>/dev/null || echo "Test data may already exist"

echo "BigQuery test environment setup complete!"