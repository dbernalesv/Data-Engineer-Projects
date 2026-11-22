CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_order_reviews (
  review_id STRING,
  order_id STRING,
  review_score STRING,
  review_comment_title STRING,
  review_comment_message STRING,
  review_creation_date STRING,
  review_answer_timestamp STRING,
  _rescued_data STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
)
USING DELTA;
