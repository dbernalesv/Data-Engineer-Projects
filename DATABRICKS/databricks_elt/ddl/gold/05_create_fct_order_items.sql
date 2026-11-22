CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fct_order_items (
  order_id STRING,
  order_item_id INT,
  customer_id STRING,
  product_id STRING,
  seller_id STRING,
  order_date_key INT,
  order_status STRING,
  item_price DECIMAL(12, 2),
  freight_value DECIMAL(12, 2),
  total_item_amount DECIMAL(13, 2),
  item_quantity INT
)
USING DELTA;
