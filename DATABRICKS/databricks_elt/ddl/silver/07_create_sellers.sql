CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_sellers (
  seller_id STRING,
  seller_zip_code_prefix STRING,
  seller_city STRING,
  seller_state STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP,
  _bronze_commit_version BIGINT,
  _bronze_commit_timestamp TIMESTAMP
)
USING DELTA;
