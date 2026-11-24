CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_seller (
  seller_id STRING,
  seller_zip_code_prefix STRING,
  seller_city STRING,
  seller_state STRING
)
USING DELTA;
