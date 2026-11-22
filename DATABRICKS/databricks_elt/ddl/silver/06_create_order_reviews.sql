CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_order_reviews (
  review_id STRING,
  order_id STRING,
  review_score INT,
  review_comment_title STRING,
  review_comment_message STRING,
  review_creation_date TIMESTAMP,
  review_answer_timestamp TIMESTAMP,
  _ingested_at TIMESTAMP
)
USING DELTA;
