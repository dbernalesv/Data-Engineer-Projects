CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_products (
  product_id STRING,
  product_category_name STRING,
  product_name_lenght STRING,
  product_description_lenght STRING,
  product_photos_qty STRING,
  product_weight_g STRING,
  product_length_cm STRING,
  product_height_cm STRING,
  product_width_cm STRING,
  _rescued_data STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
