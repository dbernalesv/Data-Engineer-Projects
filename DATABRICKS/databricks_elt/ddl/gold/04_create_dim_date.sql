CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_date (
  date_key INT,
  full_date DATE,
  year INT,
  quarter INT,
  month INT,
  day INT,
  day_of_week INT
)
USING DELTA;
