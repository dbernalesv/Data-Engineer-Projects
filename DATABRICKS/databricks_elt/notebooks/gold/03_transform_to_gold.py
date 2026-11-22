# Databricks notebook source
# MAGIC %md
# MAGIC # Modelo dimensional de Silver a Gold
# MAGIC
# MAGIC Gold publica una tabla de hechos de líneas de pedido y dimensiones para análisis. Lee CDF de Silver y registra el avance por tabla fuente.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ubicar el proyecto
# MAGIC
# MAGIC El Job entrega la ruta del bundle para importar el proceso y su configuración.

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
# MAGIC ## Cargar la configuración de Gold
# MAGIC
# MAGIC El modelo usa las tablas Silver y el esquema Gold definidos para el proyecto.

# COMMAND ----------
from databricks_elt.process.gold.transform import transform_all
from databricks_elt.utils.config_loader import load_yaml
from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)
COMMON_CONFIG = load_yaml(PROJECT_ROOT / "config" / "common.yml")
GOLD_CONFIG = load_yaml(PROJECT_ROOT / "config" / "gold.yml")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Actualizar el modelo estrella
# MAGIC
# MAGIC El hecho tiene una fila por artículo de pedido y se relaciona con cliente, producto, vendedor y fecha. Los cambios se aplican con `MERGE` idempotente.

# COMMAND ----------
transform_all(spark, PROJECT_ROOT, COMMON_CONFIG, GOLD_CONFIG)
LOGGER.info("Terminó la actualización de Gold.")
