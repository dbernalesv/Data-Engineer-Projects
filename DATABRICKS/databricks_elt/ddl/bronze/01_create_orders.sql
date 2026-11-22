CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_orders (
  order_id STRING,
  customer_id STRING,
  order_status STRING,
  order_purchase_timestamp STRING,
  order_approved_at STRING,
  order_delivered_carrier_date STRING,
  order_delivered_customer_date STRING,
  order_estimated_delivery_date STRING,
  _rescued_data STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
