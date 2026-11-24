ALTER TABLE {catalog}.{bronze_schema}.brz_orders
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{bronze_schema}.brz_order_items
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{bronze_schema}.brz_customers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{bronze_schema}.brz_products
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{bronze_schema}.brz_order_payments
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{bronze_schema}.brz_order_reviews
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{bronze_schema}.brz_sellers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{bronze_schema}.brz_category_translation
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
