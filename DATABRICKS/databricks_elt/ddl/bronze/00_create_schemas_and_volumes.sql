CREATE CATALOG IF NOT EXISTS {catalog};
CREATE SCHEMA IF NOT EXISTS {catalog}.{control_schema};
CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema};
CREATE VOLUME IF NOT EXISTS {catalog}.{control_schema}.{raw_volume};
CREATE VOLUME IF NOT EXISTS {catalog}.{control_schema}.{state_volume};
