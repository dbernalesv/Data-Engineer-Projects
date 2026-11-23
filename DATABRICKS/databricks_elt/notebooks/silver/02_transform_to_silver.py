# Databricks notebook source
# MAGIC %md
# MAGIC # Limpieza de Bronze a Silver
# MAGIC
# MAGIC Este notebook lee los cambios de Bronze con Change Data Feed (CDF), corrige tipos y aplica reglas básicas de calidad.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ubicar el proyecto
# MAGIC
# MAGIC El Job entrega la ruta del bundle para poder importar la lógica y leer los archivos YAML.

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
# MAGIC ## Cargar la configuración de Silver

# COMMAND ----------
from process.silver.transform import transform_all
from utils.config_loader import load_yaml
from utils.logging_config import get_logger

LOGGER = get_logger(__name__)
COMMON_CONFIG = load_yaml(PROJECT_ROOT / "config" / "common.yml")
SILVER_CONFIG = load_yaml(PROJECT_ROOT / "config" / "silver.yml")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aplicar las reglas y guardar los cambios
# MAGIC
# MAGIC La primera ejecución carga el estado de Bronze en una versión concreta. Las siguientes leen los commits nuevos desde la tabla de control.
# MAGIC Se consideran las imágenes posteriores a inserciones y actualizaciones; el `MERGE` evita duplicados y escrituras sin cambios.

# COMMAND ----------
if not transform_all(spark, COMMON_CONFIG, SILVER_CONFIG):
    message = "No se detectaron cambios elegibles en las tablas Bronze"
    LOGGER.info(message)
    dbutils.notebook.exit(message)
LOGGER.info("Terminó la preparación de la capa Silver.")
