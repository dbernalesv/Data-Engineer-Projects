CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_order_payments (
  order_id STRING,
  payment_sequential INT,
  payment_type STRING,
  payment_installments INT,
  payment_value DECIMAL(12, 2),
  _source_file STRING,
  _ingested_at TIMESTAMP,
  _bronze_commit_version BIGINT,
  _bronze_commit_timestamp TIMESTAMP
)
USING DELTA;
