"""Prepara Silver con lecturas incrementales desde CDF de Bronze."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from databricks_elt.utils.cdf import (
    get_watermark,
    latest_table_version,
    read_cdf_range,
    save_watermark,
)
from databricks_elt.utils.ddl import execute_ddl_directory
from databricks_elt.utils.logging_config import get_logger
from databricks_elt.utils.merge import merge_idempotently

LOGGER = get_logger(__name__)
_SQL_TYPES = {
    "integer": "INT",
    "decimal": "DECIMAL(12, 2)",
    "timestamp": "TIMESTAMP",
}
_LEGACY_COLUMNS = {
    "_source_file",
    "_bronze_commit_version",
    "_bronze_commit_timestamp",
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

    selected.extend([F.col("_ingested_at")])
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


def _latest_change_per_key(
    dataframe: DataFrame,
    primary_keys: list[str],
) -> DataFrame:
    order = [
        F.col("_commit_version").desc(),
        F.col("_commit_timestamp").desc(),
        F.when(F.col("_change_type") == "update_postimage", 1)
        .otherwise(0)
        .desc(),
    ]
    window = Window.partitionBy(*primary_keys).orderBy(*order)
    return (
        dataframe.select("*", F.row_number().over(window).alias("_change_rank"))
        .filter(F.col("_change_rank") == 1)
        .drop("_change_rank")
    )


def _prepare_table(
    spark: SparkSession,
    source_name: str,
    target_name: str,
    watermark_table: str,
    table_config: Mapping[str, object],
) -> None:
    keys = list(table_config.get("primary_keys", []))
    if not keys:
        raise ValueError(f"La tabla {target_name} debe declarar sus claves")

    latest = latest_table_version(spark, source_name)
    watermark = get_watermark(spark, watermark_table, source_name)

    if watermark is None:
        LOGGER.info(
            "Cargando el estado actual de %s en %s hasta la versión %s",
            source_name,
            target_name,
            latest.version,
        )
        snapshot = (
            spark.read.option("versionAsOf", latest.version).table(source_name)
        )
        cleaned = _silver_projection(snapshot, table_config["fields"])
        cleaned = _apply_quality_rules(cleaned, table_config).dropDuplicates(keys)
        merge_idempotently(spark, cleaned, target_name, keys)
        save_watermark(
            spark, watermark_table, source_name, target_name, latest
        )
        return

    if latest.version <= watermark.version:
        LOGGER.info(
            "%s ya está al día en la versión %s",
            target_name,
            watermark.version,
        )
        return

    LOGGER.info(
        "Leyendo CDF de %s desde %s hasta %s",
        source_name,
        watermark.version + 1,
        latest.version,
    )
    changes = read_cdf_range(
        spark, source_name, watermark.version + 1, latest.version
    ).filter(F.col("_change_type").isin("insert", "update_postimage"))
    changes = _latest_change_per_key(changes, keys)
    cleaned = _silver_projection(changes, table_config["fields"])
    cleaned = _apply_quality_rules(cleaned, table_config)
    merge_idempotently(spark, cleaned, target_name, keys)
    save_watermark(spark, watermark_table, source_name, target_name, latest)
    LOGGER.info("Terminó %s en la versión %s", target_name, latest.version)


def _rebuild_legacy_tables(
    spark: SparkSession,
    project_root: Path,
    identifiers: Mapping[str, str],
    common_config: Mapping[str, str],
    silver_config: Mapping[str, object],
    tables: list[Mapping[str, object]],
) -> None:
    catalog = common_config["catalog"]
    target_schema = str(silver_config["target_schema"]).format(**common_config)
    watermark_table = (
        f"{catalog}.{common_config['control_schema']}."
        f"{common_config['cdf_watermarks_table']}"
    )
    legacy_tables = []

    for table_config in tables:
        target_name = f"{catalog}.{target_schema}.{table_config['target_table']}"
        if not spark.catalog.tableExists(target_name):
            continue
        legacy = _LEGACY_COLUMNS.intersection(spark.table(target_name).columns)
        if legacy:
            legacy_tables.append((target_name, table_config["source_table"]))

    if not legacy_tables:
        return

    for target_name, source_table in legacy_tables:
        LOGGER.warning(
            "Reconstruyendo %s desde Bronze para retirar columnas antiguas",
            target_name,
        )
        spark.sql(f"DROP TABLE IF EXISTS {target_name}")
        spark.sql(
            f"DELETE FROM {watermark_table} "
            f"WHERE source_table = '{catalog}.{common_config['bronze_schema']}."
            f"{source_table}'"
        )

    execute_ddl_directory(spark, project_root / "ddl" / "silver", identifiers)


def transform_all(
    spark: SparkSession,
    project_root: Path,
    common_config: Mapping[str, str],
    silver_config: Mapping[str, object],
) -> None:
    """Procesa CDF por rango y registra el avance de cada tabla."""
    identifiers = {
        "catalog": common_config["catalog"],
        "silver_schema": common_config["silver_schema"],
        "control_schema": common_config["control_schema"],
        "cdf_watermarks_table": common_config["cdf_watermarks_table"],
    }
    execute_ddl_directory(spark, project_root / "ddl" / "silver", identifiers)
    execute_ddl_directory(spark, project_root / "ddl" / "control", identifiers)

    tables = silver_config.get("tables", [])
    if not tables:
        raise ValueError("La configuración Silver debe incluir al menos una tabla")

    _rebuild_legacy_tables(
        spark,
        project_root,
        identifiers,
        common_config,
        silver_config,
        tables,
    )

    catalog = common_config["catalog"]
    source_schema = str(silver_config["source_schema"]).format(**common_config)
    target_schema = str(silver_config["target_schema"]).format(**common_config)
    watermark_table = (
        f"{catalog}.{common_config['control_schema']}."
        f"{common_config['cdf_watermarks_table']}"
    )

    for table_config in tables:
        source_name = f"{catalog}.{source_schema}.{table_config['source_table']}"
        target_name = f"{catalog}.{target_schema}.{table_config['target_table']}"
        try:
            _prepare_table(
                spark,
                source_name,
                target_name,
                watermark_table,
                table_config,
            )
        except Exception:
            LOGGER.exception("Falló la preparación de %s", target_name)
            raise
