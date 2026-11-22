"""Ejecuta los archivos DDL en orden y valida sus identificadores SQL."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

from pyspark.sql import SparkSession

from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def execute_ddl_directory(
    spark: SparkSession,
    ddl_directory: Path,
    identifiers: Mapping[str, str],
) -> None:
    """Aplica, en orden, las sentencias separadas por punto y coma."""
    for key, value in identifiers.items():
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"El identificador SQL de {key} no es válido: {value!r}")

    ddl_files = sorted(ddl_directory.glob("*.sql"))
    if not ddl_files:
        raise FileNotFoundError(f"No se encontraron archivos DDL en {ddl_directory}")

    for ddl_file in ddl_files:
        sql_text = ddl_file.read_text(encoding="utf-8").format(**identifiers)
        statements = [statement.strip() for statement in sql_text.split(";")]
        for statement in filter(None, statements):
            LOGGER.info("Aplicando sentencias de %s", ddl_file.name)
            spark.sql(statement)
