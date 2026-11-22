ALTER TABLE {catalog}.{silver_schema}.slv_orders
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{silver_schema}.slv_order_items
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{silver_schema}.slv_customers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{silver_schema}.slv_products
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{silver_schema}.slv_order_payments
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{silver_schema}.slv_order_reviews
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{silver_schema}.slv_sellers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE {catalog}.{silver_schema}.slv_category_translation
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
