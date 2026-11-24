CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_orders (
  order_id STRING,
  customer_id STRING,
  order_status STRING,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP,
  _ingested_at TIMESTAMP
)
USING DELTA;
