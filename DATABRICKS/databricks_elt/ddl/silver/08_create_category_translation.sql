CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_category_translation (
  product_category_name STRING,
  product_category_name_english STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
