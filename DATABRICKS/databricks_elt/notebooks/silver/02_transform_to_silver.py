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
from databricks_elt.process.silver.transform import transform_all
from databricks_elt.utils.config_loader import load_yaml
from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)
COMMON_CONFIG = load_yaml(PROJECT_ROOT / "config" / "common.yml")
SILVER_CONFIG = load_yaml(PROJECT_ROOT / "config" / "silver.yml")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aplicar las reglas y guardar los cambios
# MAGIC
# MAGIC La primera lectura CDF carga el snapshot disponible. Las siguientes procesan las nuevas inserciones desde su checkpoint.
# MAGIC Bronze es append-only en esta etapa, por lo que Silver consume los eventos `insert` del CDF.

# COMMAND ----------
transform_all(spark, PROJECT_ROOT, COMMON_CONFIG, SILVER_CONFIG)
LOGGER.info("Terminó la preparación de la capa Silver.")
