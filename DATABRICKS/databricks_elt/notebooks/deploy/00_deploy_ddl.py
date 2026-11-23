# Databricks notebook source
# MAGIC %md
# MAGIC # Desplegar las estructuras de Olist
# MAGIC
# MAGIC El manifiesto define el orden de los archivos SQL que se aplican al desplegar.

# COMMAND ----------
import sys
from pathlib import Path

dbutils.widgets.text("project_root", "")
dbutils.widgets.text("manifest_path", "deploy/initial.yml")
root_value = dbutils.widgets.get("project_root").strip()
if not root_value:
    raise ValueError("Falta el parámetro project_root del Job")

project_root = Path(root_value)
sys.path.insert(0, str(project_root / "src"))

# COMMAND ----------
from utils.config_loader import load_yaml
from utils.ddl import deploy_manifest

common = load_yaml(project_root / "config" / "common.yml")
deploy_manifest(spark, project_root, dbutils.widgets.get("manifest_path"), common)
