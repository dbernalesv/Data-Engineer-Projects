# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta de Olist a Raw
# MAGIC
# MAGIC Copia los CSV originales desde la bandeja de entrada y valida su contenido antes de iniciar Bronze.

# COMMAND ----------
import json
import sys
from pathlib import Path

dbutils.widgets.text("project_root", "")
project_root_value = dbutils.widgets.get("project_root").strip()
if not project_root_value:
    raise ValueError("Falta el parámetro project_root del Job")

PROJECT_ROOT = Path(project_root_value)
SOURCE_ROOT = str(PROJECT_ROOT / "src")
if SOURCE_ROOT not in sys.path:
    sys.path.insert(0, SOURCE_ROOT)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Preparar el volumen y copiar los archivos

# COMMAND ----------
from databricks_elt.process.raw.ingest import ingest_files
from databricks_elt.utils.config_loader import load_yaml
from databricks_elt.utils.ddl import execute_ddl_directory
from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)
common = load_yaml(PROJECT_ROOT / "config" / "common.yml")
raw_config = load_yaml(PROJECT_ROOT / "config" / "raw.yml")
bronze_config = load_yaml(PROJECT_ROOT / "config" / "bronze.yml")
identifiers = {key: common[key] for key in ("catalog", "control_schema", "raw_volume")}

execute_ddl_directory(spark, PROJECT_ROOT / "ddl" / "raw", identifiers)
result = ingest_files(
    Path(raw_config["inbox_dir"].format(**common)),
    Path(bronze_config["source_dir"].format(**common)),
    raw_config["files"],
)
LOGGER.info("Ingesta Raw finalizada: %s", result)
dbutils.notebook.exit(json.dumps(result))
