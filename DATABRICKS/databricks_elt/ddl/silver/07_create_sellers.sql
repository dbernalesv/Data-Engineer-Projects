CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_sellers (
  seller_id STRING,
  seller_zip_code_prefix STRING,
  seller_city STRING,
  seller_state STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
