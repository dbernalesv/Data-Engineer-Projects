# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta de Olist a Bronze
# MAGIC
# MAGIC Este notebook prepara las estructuras de Bronze y carga los CSV desde el volumen Raw.
# MAGIC La configuración de las rutas y los archivos está en `config/`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ubicar el proyecto
# MAGIC
# MAGIC El Job entrega la ruta del proyecto como parámetro. Desde allí se cargan los módulos y la configuración.

# COMMAND ----------
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
# MAGIC ## Leer la configuración y preparar la carga

# COMMAND ----------
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Crear o comprobar las estructuras Bronze
# MAGIC
# MAGIC El DDL es idempotente: si los esquemas, volúmenes o tablas ya existen, se conservan.

# COMMAND ----------
execute_ddl_directory(spark, PROJECT_ROOT / "ddl" / "raw", IDENTIFIERS)

execute_ddl_directory(
    spark,
    PROJECT_ROOT / "ddl" / "bronze",
    IDENTIFIERS,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cargar los archivos Raw
# MAGIC
# MAGIC Auto Loader procesa los archivos definidos en `config/bronze.yml` y guarda su estado en el volumen de control.

# COMMAND ----------
ingest_all(spark, COMMON_CONFIG, BRONZE_CONFIG)
LOGGER.info("Terminó la carga de los archivos Raw hacia Bronze.")
