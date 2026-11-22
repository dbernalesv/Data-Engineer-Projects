"""Apply ordered SQL DDL files with validated identifier substitutions."""

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
    """Run semicolon-separated DDL statements in lexicographic file order."""
    for key, value in identifiers.items():
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"Invalid SQL identifier for {key}: {value!r}")

    ddl_files = sorted(ddl_directory.glob("*.sql"))
    if not ddl_files:
        raise FileNotFoundError(f"No DDL files found in {ddl_directory}")

    for ddl_file in ddl_files:
        sql_text = ddl_file.read_text(encoding="utf-8").format(**identifiers)
        statements = [statement.strip() for statement in sql_text.split(";")]
        for statement in filter(None, statements):
            LOGGER.info("Applying DDL from %s", ddl_file.name)
            spark.sql(statement)
