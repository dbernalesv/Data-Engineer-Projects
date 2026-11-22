# Databricks notebook source
"""Create Bronze objects and ingest the configured Olist CSV files."""

import sys
from pathlib import Path

dbutils.widgets.text("project_root", "")
project_root_value = dbutils.widgets.get("project_root").strip()
if not project_root_value:
    raise ValueError("The project_root job parameter is required")

PROJECT_ROOT = Path(project_root_value)
SOURCE_ROOT = str(PROJECT_ROOT / "src")
if SOURCE_ROOT not in sys.path:
    sys.path.insert(0, SOURCE_ROOT)

# Import project modules after adding the deployed src directory to the path.
from databricks_elt.process.bronze.ingest import ingest_all
from databricks_elt.utils.config_loader import load_yaml
from databricks_elt.utils.ddl import execute_ddl_directory
from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)
COMMON_CONFIG = load_yaml(PROJECT_ROOT / "config" / "common.yml")
BRONZE_CONFIG = load_yaml(PROJECT_ROOT / "config" / "bronze.yml")

IDENTIFIERS = {
    "catalog": COMMON_CONFIG["catalog"],
    "control_schema": COMMON_CONFIG["control_schema"],
    "raw_volume": COMMON_CONFIG["raw_volume"],
    "state_volume": COMMON_CONFIG["state_volume"],
    "bronze_schema": COMMON_CONFIG["bronze_schema"],
}

execute_ddl_directory(
    spark,
    PROJECT_ROOT / "ddl" / "bronze",
    IDENTIFIERS,
)
ingest_all(spark, COMMON_CONFIG, BRONZE_CONFIG)
LOGGER.info("Raw-to-Bronze ingestion completed")
