"""Carga los CSV de Olist en las tablas Delta de Bronze."""

from __future__ import annotations

from collections.abc import Mapping
from fnmatch import fnmatchcase
from pathlib import Path
from urllib.parse import unquote

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.errors import PySparkException

from utils.logging_config import get_logger

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


def _file_state(
    spark: SparkSession, checkpoint: Path
) -> tuple[set[str], set[str]]:
    """Devuelve los archivos conocidos y los que el checkpoint confirma."""
    if not (checkpoint / "metadata").is_file():
        return set(), set()
    try:
        state = spark.sql("SELECT * FROM cloud_files_state(:checkpoint)", {
            "checkpoint": str(checkpoint),
        })
        processed = F.lit(False)
        if "commit_time" in state.columns:
            processed = F.col("commit_time").isNotNull()
        if "ingestion_state" in state.columns:
            processed = processed | F.col("ingestion_state").isin("INGESTED", "SKIPPED_CORRUPTED")
        rows = state.select("path", processed.alias("processed")).collect()
    except PySparkException:
        LOGGER.warning("Estado del checkpoint no disponible en %s; Auto Loader verificará la ruta", checkpoint)
        return set(), set()
    known = set()
    committed = set()
    for row in rows:
        name = Path(unquote(row["path"])).name
        known.add(name)
        if row["processed"]:
            committed.add(name)
    return known, committed


def _stream_progress(spark: SparkSession, table_name: str, previous: int) -> bool:
    """Verifica si una escritura nueva del stream añadió filas a Delta."""
    history = spark.sql(f"DESCRIBE HISTORY {table_name}")
    return bool(
        history.filter(
            (F.col("version") > previous)
            & (F.col("operation") == "STREAMING UPDATE")
            & (F.col("operationMetrics")["numOutputRows"].cast("long") > 0)
        ).select("version").take(1)
    )


def _ingest_table(
    spark: SparkSession,
    common_config: Mapping[str, str],
    bronze_config: Mapping[str, object],
    table_config: Mapping[str, str],
) -> bool:
    catalog = common_config["catalog"]
    schema = str(bronze_config["target_schema"]).format(**common_config)
    table = table_config["target_table"]
    table_name = _table_name(catalog, schema, table)
    source_dir = Path(str(bronze_config["source_dir"]).format(**common_config)) / table_config["subdirectory"]
    table_state_dir = Path(str(bronze_config["state_dir"]).format(**common_config)) / table
    checkpoint = table_state_dir / "checkpoint"
    file_format = str(bronze_config["file_format"])
    pattern = table_config["source_file_pattern"]
    candidates = {
        path.name for path in source_dir.iterdir() if path.is_file() and fnmatchcase(path.name, pattern)
    } if source_dir.is_dir() else set()
    if not candidates:
        LOGGER.info("%s: No se detectaron archivos nuevos", table_name)
        return False

    known, committed = _file_state(spark, checkpoint)
    pending = candidates - committed
    if not pending:
        LOGGER.info("%s: No se detectaron archivos nuevos", table_name)
        return False

    LOGGER.info("%s: Verificando archivos pendientes con Auto Loader", table_name)
    previous_version = int(spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").first()["version"])

    source = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{table_state_dir}/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", str(bronze_config["header"]).lower())
        .option("multiLine", str(bronze_config["multiline"]).lower())
        .option("pathGlobFilter", pattern)
        .load(str(source_dir))
    )

    query = (
        _with_ingestion_metadata(source)
        .writeStream.format("delta")
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .toTable(table_name)
    )

    try:
        query.awaitTermination()
    except Exception:
        LOGGER.exception("Falló la carga de %s con Auto Loader", table_name)
        raise

    if _stream_progress(spark, table_name, previous_version):
        LOGGER.info("%s: Se detectaron archivos nuevos para procesar", table_name)
        return True

    discovered, confirmed = _file_state(spark, checkpoint)
    if pending & ((discovered - known) | (confirmed - committed)):
        LOGGER.info("%s: Se detectaron archivos nuevos sin registros", table_name)
        return True

    LOGGER.info("%s: No se detectaron archivos nuevos", table_name)
    return False


def ingest_all(
    spark: SparkSession,
    common_config: Mapping[str, str],
    bronze_config: Mapping[str, object],
) -> bool:
    """Procesa los archivos configurados, uno por uno y con estado propio."""
    tables = bronze_config.get("tables", [])
    if not tables:
        raise ValueError("La configuración Bronze debe incluir al menos una tabla de origen")

    processed = False
    for table_config in tables:
        processed = _ingest_table(spark, common_config, bronze_config, table_config) or processed
    return processed
