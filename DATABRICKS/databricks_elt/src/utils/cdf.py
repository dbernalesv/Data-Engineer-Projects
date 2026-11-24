"""Lectura incremental de CDF y control de versiones por tabla."""

from __future__ import annotations

from datetime import datetime
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from utils.merge import merge_idempotently


class TableVersion(NamedTuple):
    version: int
    timestamp: datetime


def latest_table_version(spark: SparkSession, table_name: str) -> TableVersion:
    """Obtiene la versión y fecha del último commit Delta."""
    latest = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").first()
    if latest is None:
        raise RuntimeError(f"No se encontró historial para {table_name}")
    return TableVersion(int(latest["version"]), latest["timestamp"])


def get_watermark(
    spark: SparkSession,
    watermark_table: str,
    source_table: str,
) -> TableVersion | None:
    """Busca la última versión procesada para una tabla fuente."""
    row = (
        spark.table(watermark_table)
        .filter(F.col("source_table") == source_table)
        .select("last_processed_commit_version", "last_processed_commit_timestamp")
        .first()
    )
    if row is None:
        return None
    return TableVersion(
        int(row["last_processed_commit_version"]),
        row["last_processed_commit_timestamp"],
    )


def save_watermark(
    spark: SparkSession,
    watermark_table: str,
    source_table: str,
    target_table: str,
    version: TableVersion,
) -> None:
    """Guarda el avance solo después de completar la escritura destino."""
    watermark = spark.createDataFrame(
        [
            (
                source_table,
                target_table,
                version.version,
                version.timestamp,
            )
        ],
        schema=(
            "source_table STRING, target_table STRING, "
            "last_processed_commit_version BIGINT, "
            "last_processed_commit_timestamp TIMESTAMP"
        ),
    ).select("*", F.current_timestamp().alias("updated_at"))
    merge_idempotently(
        spark,
        watermark,
        watermark_table,
        ["source_table"],
    )


def read_cdf_range(
    spark: SparkSession,
    table_name: str,
    starting_version: int,
    ending_version: int,
) -> DataFrame:
    """Lee un rango cerrado de versiones CDF de una tabla Delta."""
    return (
        spark.read.option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
        .option("endingVersion", ending_version)
        .table(table_name)
    )


def latest_change_images(dataframe: DataFrame, primary_keys: list[str]) -> DataFrame:
    """Conserva la imagen más reciente por clave, incluida la posterior a updates."""
    window = Window.partitionBy(*primary_keys).orderBy(
        F.col("_commit_version").desc(),
        F.col("_commit_timestamp").desc(),
        F.when(F.col("_change_type") == "update_postimage", 1)
        .otherwise(0)
        .desc(),
    )
    return (
        dataframe.filter(F.col("_change_type").isin("insert", "update_postimage"))
        .select("*", F.row_number().over(window).alias("_change_rank"))
        .filter(F.col("_change_rank") == 1)
        .drop("_change_rank")
    )
