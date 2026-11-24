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
from process.raw.ingest import ingest_files
from utils.config_loader import load_yaml
from utils.logging_config import get_logger

LOGGER = get_logger(__name__)
common = load_yaml(PROJECT_ROOT / "config" / "common.yml")
raw_config = load_yaml(PROJECT_ROOT / "config" / "raw.yml")
bronze_config = load_yaml(PROJECT_ROOT / "config" / "bronze.yml")
inbox = Path(raw_config["inbox_dir"].format(**common))
result = ingest_files(
    inbox,
    Path(bronze_config["source_dir"].format(**common)),
    bronze_config["tables"],
)
LOGGER.info("Ingesta Raw finalizada: %s", result)
dbutils.notebook.exit(json.dumps(result))
