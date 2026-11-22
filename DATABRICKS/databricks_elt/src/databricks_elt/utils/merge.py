"""Operaciones Delta reutilizables para escrituras idempotentes."""

from __future__ import annotations

import re
import uuid

from pyspark.sql import DataFrame, SparkSession

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(identifier: str) -> str:
    parts = identifier.split(".")
    if not all(_IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"Identificador SQL no válido: {identifier!r}")
    return ".".join(f"`{part}`" for part in parts)


def merge_idempotently(
    spark: SparkSession,
    source: DataFrame,
    target_name: str,
    primary_keys: list[str],
) -> None:
    """Inserta o actualiza por clave, evitando escrituras si nada cambió."""
    if not primary_keys:
        raise ValueError(f"La tabla {target_name} debe declarar sus claves")

    columns = source.columns
    missing_keys = set(primary_keys).difference(columns)
    if missing_keys:
        raise ValueError(f"Faltan claves en el origen de {target_name}: {missing_keys}")

    target = _quote_identifier(target_name)
    quoted_columns = {column: _quote_identifier(column) for column in columns}
    quoted_keys = [quoted_columns[key] for key in primary_keys]
    view_name = f"merge_source_{uuid.uuid4().hex}"
    source.createOrReplaceTempView(view_name)

    match = " AND ".join(
        f"target.{key} <=> source.{key}" for key in quoted_keys
    )
    updates = [column for column in columns if column not in primary_keys]
    update_set = ", ".join(
        f"{quoted_columns[column]} = source.{quoted_columns[column]}"
        for column in updates
    )
    changed = " OR ".join(
        f"NOT (target.{quoted_columns[column]} <=> "
        f"source.{quoted_columns[column]})"
        for column in updates
    )
    insert_columns = ", ".join(quoted_columns.values())
    insert_values = ", ".join(
        f"source.{quoted_columns[column]}" for column in columns
    )
    matched_clause = (
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {update_set}"
        if updates
        else ""
    )

    try:
        spark.sql(
            f"MERGE INTO {target} AS target "
            f"USING `{view_name}` AS source ON {match} "
            f"{matched_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) "
            f"VALUES ({insert_values})"
        )
    finally:
        spark.catalog.dropTempView(view_name)
