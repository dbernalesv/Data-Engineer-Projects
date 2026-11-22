CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_order_payments (
  order_id STRING,
  payment_sequential STRING,
  payment_type STRING,
  payment_installments STRING,
  payment_value STRING,
  _rescued_data STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
