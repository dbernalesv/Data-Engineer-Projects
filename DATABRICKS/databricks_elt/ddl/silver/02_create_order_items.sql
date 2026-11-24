CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_order_items (
  order_id STRING,
  order_item_id INT,
  product_id STRING,
  seller_id STRING,
  shipping_limit_date TIMESTAMP,
  price DECIMAL(12, 2),
  freight_value DECIMAL(12, 2),
  _ingested_at TIMESTAMP
)
USING DELTA;
