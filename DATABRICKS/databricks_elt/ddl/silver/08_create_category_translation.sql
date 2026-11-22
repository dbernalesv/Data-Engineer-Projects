CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_category_translation (
  product_category_name STRING,
  product_category_name_english STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP,
  _bronze_commit_version BIGINT,
  _bronze_commit_timestamp TIMESTAMP
)
USING DELTA;
