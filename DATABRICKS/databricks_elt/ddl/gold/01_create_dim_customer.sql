CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_customer (
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,
  customer_city STRING,
  customer_state STRING
)
USING DELTA;
