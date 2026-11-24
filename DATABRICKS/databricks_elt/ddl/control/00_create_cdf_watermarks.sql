CREATE SCHEMA IF NOT EXISTS {catalog}.{control_schema};

CREATE TABLE IF NOT EXISTS {catalog}.{control_schema}.{cdf_watermarks_table} (
  source_table STRING,
  target_table STRING,
  last_processed_commit_version BIGINT,
  last_processed_commit_timestamp TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA;
