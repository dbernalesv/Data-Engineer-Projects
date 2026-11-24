"""Prepara Silver con lecturas incrementales desde CDF de Bronze."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession, functions as F

from utils.cdf import (
    get_watermark,
    latest_change_images,
    latest_table_version,
    read_cdf_range,
    save_watermark,
)
from utils.logging_config import get_logger
from utils.merge import merge_idempotently

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


def _prepare_table(
    spark: SparkSession,
    source_name: str,
    target_name: str,
    watermark_table: str,
    table_config: Mapping[str, object],
) -> bool:
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
        has_rows = not cleaned.isEmpty()
        if has_rows:
            merge_idempotently(spark, cleaned, target_name, keys)
        save_watermark(
            spark, watermark_table, source_name, target_name, latest
        )
        return has_rows

    if latest.version <= watermark.version:
        LOGGER.info(
            "No se detectaron cambios en la tabla Bronze %s (versión %s)",
            source_name,
            watermark.version,
        )
        return False

    LOGGER.info(
        "Leyendo CDF de %s desde %s hasta %s",
        source_name,
        watermark.version + 1,
        latest.version,
    )
    changes = latest_change_images(
        read_cdf_range(spark, source_name, watermark.version + 1, latest.version),
        keys,
    )
    cleaned = _silver_projection(changes, table_config["fields"])
    cleaned = _apply_quality_rules(cleaned, table_config)
    has_rows = not cleaned.isEmpty()
    if has_rows:
        merge_idempotently(spark, cleaned, target_name, keys)
    else:
        LOGGER.info("%s no tiene cambios elegibles; se actualiza el watermark", source_name)
    save_watermark(spark, watermark_table, source_name, target_name, latest)
    LOGGER.info("Terminó %s en la versión %s", target_name, latest.version)
    return has_rows


def transform_all(
    spark: SparkSession,
    common_config: Mapping[str, str],
    silver_config: Mapping[str, object],
) -> bool:
    """Procesa CDF por rango y registra el avance de cada tabla."""
    tables = silver_config.get("tables", [])
    if not tables:
        raise ValueError("La configuración Silver debe incluir al menos una tabla")

    catalog = common_config["catalog"]
    source_schema = str(silver_config["source_schema"]).format(**common_config)
    target_schema = str(silver_config["target_schema"]).format(**common_config)
    watermark_table = (
        f"{catalog}.{common_config['control_schema']}."
        f"{common_config['cdf_watermarks_table']}"
    )

    processed = False
    for table_config in tables:
        source_name = f"{catalog}.{source_schema}.{table_config['source_table']}"
        target_name = f"{catalog}.{target_schema}.{table_config['target_table']}"
        try:
            updated = _prepare_table(
                spark,
                source_name,
                target_name,
                watermark_table,
                table_config,
            )
            processed = processed or updated
        except Exception:
            LOGGER.exception("Falló la preparación de %s", target_name)
            raise
    return processed
