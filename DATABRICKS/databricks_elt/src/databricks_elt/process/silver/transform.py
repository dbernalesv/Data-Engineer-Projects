"""Limpia y tipa los cambios que llegan desde Bronze."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from databricks_elt.utils.ddl import execute_ddl_directory
from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)
_SQL_TYPES = {
    "integer": "INT",
    "decimal": "DECIMAL(12, 2)",
    "timestamp": "TIMESTAMP",
}


def _silver_projection(dataframe: DataFrame, fields: list[Mapping[str, str]]) -> DataFrame:
    selected = []
    for field in fields:
        source = field["source"]
        target = field["target"]
        data_type = field["type"]

        if data_type == "string":
            value = F.trim(F.col(f"`{source}`"))
            if field.get("normalize") == "lower":
                value = F.lower(value)
            elif field.get("normalize") == "upper":
                value = F.upper(value)
        else:
            sql_type = _SQL_TYPES[data_type]
            value = F.expr(f"try_cast(trim(`{source}`) AS {sql_type})")

        selected.append(value.alias(target))

    selected.extend(
        [
            F.col("_source_file"),
            F.col("_ingested_at"),
            F.col("_commit_version").alias("_bronze_commit_version"),
            F.col("_commit_timestamp").alias("_bronze_commit_timestamp"),
        ]
    )
    return dataframe.select(*selected)


def _apply_quality_rules(
    dataframe: DataFrame,
    table_config: Mapping[str, object],
) -> DataFrame:
    for field in table_config.get("required_fields", []):
        value = F.col(field)
        dataframe = dataframe.filter(
            value.isNotNull() & (F.length(F.trim(value.cast("string"))) > 0)
        )

    for field in table_config.get("non_negative_fields", []):
        value = F.col(field)
        dataframe = dataframe.filter(value.isNull() | (value >= 0))

    for rule in table_config.get("ranges", []):
        value = F.col(rule["field"])
        dataframe = dataframe.filter(value.between(rule["minimum"], rule["maximum"]))

    return dataframe


def _transform_table(
    spark: SparkSession,
    common_config: Mapping[str, str],
    silver_config: Mapping[str, object],
    table_config: Mapping[str, object],
) -> None:
    catalog = common_config["catalog"]
    source_schema = str(silver_config["source_schema"]).format(**common_config)
    target_schema = str(silver_config["target_schema"]).format(**common_config)
    source_name = f"{catalog}.{source_schema}.{table_config['source_table']}"
    target_name = f"{catalog}.{target_schema}.{table_config['target_table']}"
    checkpoint = (
        f"{silver_config['state_dir'].format(**common_config)}"
        f"/{table_config['target_table']}/checkpoint"
    )

    LOGGER.info("Leyendo cambios de %s para preparar %s", source_name, target_name)
    changes = (
        spark.readStream.option("readChangeFeed", "true")
        .table(source_name)
        .filter(F.col("_change_type") == "insert")
    )
    cleaned = _silver_projection(changes, table_config["fields"])
    cleaned = _apply_quality_rules(cleaned, table_config)
    keys = table_config.get("primary_keys", [])
    if not keys:
        raise ValueError(f"La tabla {target_name} debe declarar sus claves para deduplicar")
    cleaned = cleaned.dropDuplicates(keys)

    query = (
        cleaned.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(target_name)
    )

    try:
        query.awaitTermination()
    except Exception:
        LOGGER.exception("Falló la transformación de %s", target_name)
        raise

    progress = query.lastProgress
    rows = progress.get("numInputRows", 0) if progress else 0
    LOGGER.info("Terminó %s; cambios leídos desde Bronze: %s", target_name, rows)


def transform_all(
    spark: SparkSession,
    project_root: Path,
    common_config: Mapping[str, str],
    silver_config: Mapping[str, object],
) -> None:
    """Prepara las tablas Silver y procesa sus cambios mediante CDF."""
    identifiers = {
        "catalog": common_config["catalog"],
        "silver_schema": common_config["silver_schema"],
    }
    execute_ddl_directory(spark, project_root / "ddl" / "silver", identifiers)

    tables = silver_config.get("tables", [])
    if not tables:
        raise ValueError("La configuración Silver debe incluir al menos una tabla")

    for table_config in tables:
        _transform_table(spark, common_config, silver_config, table_config)
