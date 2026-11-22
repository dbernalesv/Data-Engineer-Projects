CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_order_items (
  order_id STRING,
  order_item_id STRING,
  product_id STRING,
  seller_id STRING,
  shipping_limit_date STRING,
  price STRING,
  freight_value STRING,
  _rescued_data STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
