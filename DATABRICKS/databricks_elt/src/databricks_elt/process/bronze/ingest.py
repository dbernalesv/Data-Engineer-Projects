"""Carga los CSV de Olist en las tablas Delta de Bronze."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession, functions as F

from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)


def _table_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _with_ingestion_metadata(dataframe: DataFrame) -> DataFrame:
    source_columns = [column for column in dataframe.columns if column != "_rescued_data"]
    selected_columns = [F.col(f"`{column}`") for column in source_columns]
    selected_columns.extend(
        [
            F.col("_rescued_data"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        ]
    )
    return dataframe.select(*selected_columns)


def _ingest_table(
    spark: SparkSession,
    common_config: Mapping[str, str],
    bronze_config: Mapping[str, object],
    table_config: Mapping[str, str],
) -> None:
    catalog = common_config["catalog"]
    schema = str(bronze_config["target_schema"]).format(**common_config)
    table = table_config["target_table"]
    table_name = _table_name(catalog, schema, table)
    source_dir = str(bronze_config["source_dir"]).format(**common_config)
    state_dir = str(bronze_config["state_dir"]).format(**common_config)
    table_state_dir = f"{state_dir}/{table}"
    file_format = str(bronze_config["file_format"])

    LOGGER.info("Iniciando la carga de %s con Auto Loader", table_name)

    source = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{table_state_dir}/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", str(bronze_config["header"]).lower())
        .option("multiLine", str(bronze_config["multiline"]).lower())
        .option("pathGlobFilter", table_config["source_file_pattern"])
        .load(source_dir)
    )

    query = (
        _with_ingestion_metadata(source)
        .writeStream.format("delta")
        .option("checkpointLocation", f"{table_state_dir}/checkpoint")
        .trigger(availableNow=True)
        .toTable(table_name)
    )

    try:
        query.awaitTermination()
    except Exception:
        LOGGER.exception("Falló la carga de %s con Auto Loader", table_name)
        raise

    progress = query.lastProgress
    rows = progress.get("numInputRows", 0) if progress else 0
    LOGGER.info("Carga de %s finalizada; filas leídas: %s", table_name, rows)


def ingest_all(
    spark: SparkSession,
    common_config: Mapping[str, str],
    bronze_config: Mapping[str, object],
) -> None:
    """Procesa los archivos configurados, uno por uno y con estado propio."""
    tables = bronze_config.get("tables", [])
    if not tables:
        raise ValueError("La configuración Bronze debe incluir al menos una tabla de origen")

    for table_config in tables:
        _ingest_table(spark, common_config, bronze_config, table_config)
