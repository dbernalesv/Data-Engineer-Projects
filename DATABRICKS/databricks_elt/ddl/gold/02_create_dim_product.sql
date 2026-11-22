CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_product (
  product_id STRING,
  product_category_name STRING,
  product_category_name_english STRING,
  product_name_length INT,
  product_description_length INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
)
USING DELTA;
