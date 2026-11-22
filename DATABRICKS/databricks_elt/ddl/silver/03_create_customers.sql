CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_customers (
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,
  customer_city STRING,
  customer_state STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
