CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_category_translation (
  product_category_name STRING,
  product_category_name_english STRING,
  _rescued_data STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
