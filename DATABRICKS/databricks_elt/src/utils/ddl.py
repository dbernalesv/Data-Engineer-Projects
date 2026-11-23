"""Ejecuta los archivos DDL en orden y valida sus identificadores SQL."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

from pyspark.sql import SparkSession

from utils.config_loader import load_yaml
from utils.logging_config import get_logger

LOGGER = get_logger(__name__)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CDF_ALTER = re.compile(
    r"ALTER\s+TABLE\s+([\w.]+)\s+SET\s+TBLPROPERTIES\s*"
    r"\(\s*delta\.enableChangeDataFeed\s*=\s*true\s*\)",
    re.IGNORECASE,
)


def _project_path(root: Path, directory: str, relative: str, suffixes: tuple[str, ...]) -> Path:
    if not isinstance(relative, str) or not relative.strip():
        raise ValueError("Se esperaba una ruta relativa no vacía")
    candidate = Path(relative)
    if candidate.is_absolute() or candidate.drive or ".." in candidate.parts:
        raise ValueError(f"Ruta fuera de {directory}: {relative}")
    resolved = (root / candidate).resolve()
    if not resolved.is_relative_to((root / directory).resolve()):
        raise ValueError(f"Ruta fuera de {directory}: {relative}")
    if resolved.suffix not in suffixes or not resolved.is_file():
        raise ValueError(f"Archivo inexistente o de tipo incorrecto: {relative}")
    return resolved


def prepare_manifest(
    project_root: Path,
    manifest_path: str,
    identifiers: Mapping[str, str],
) -> list[tuple[str, list[str]]]:
    """Comprueba todos los archivos e identificadores antes de ejecutar SQL."""
    for key, value in identifiers.items():
        if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"El identificador SQL de {key} no es válido: {value!r}")
    manifest = _project_path(project_root, "deploy", manifest_path, (".yml", ".yaml"))
    files = load_yaml(manifest).get("ddl_files")
    if not isinstance(files, list) or not files:
        raise ValueError("El manifiesto debe declarar una lista ddl_files no vacía")

    seen = set()
    plan = []
    for relative in files:
        ddl_file = _project_path(project_root, "ddl", relative, (".sql",))
        if ddl_file in seen:
            raise ValueError(f"El manifiesto repite el archivo {relative}")
        seen.add(ddl_file)
        try:
            sql_text = ddl_file.read_text(encoding="utf-8").format(**identifiers)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"No se pudieron resolver los identificadores en {relative}") from exc
        statements = [piece.strip() for piece in sql_text.split(";") if piece.strip()]
        if not statements:
            raise ValueError(f"El archivo no contiene sentencias: {relative}")
        plan.append((relative, statements))
    return plan


def deploy_manifest(
    spark: SparkSession,
    project_root: Path,
    manifest_path: str,
    identifiers: Mapping[str, str],
) -> None:
    for relative, statements in prepare_manifest(project_root, manifest_path, identifiers):
        LOGGER.info("Aplicando sentencias de %s", relative)
        for index, statement in enumerate(statements, start=1):
            try:
                alter = _CDF_ALTER.fullmatch(statement)
                if alter:
                    table = alter[1]
                    prop = spark.sql(
                        f"SHOW TBLPROPERTIES {table} ('delta.enableChangeDataFeed')"
                    ).first()
                    if prop is not None and str(prop["value"]).lower() == "true":
                        LOGGER.info("CDF ya habilitado en %s", table)
                        continue
                spark.sql(statement).collect()
            except Exception as exc:
                raise RuntimeError(
                    f"Falló la sentencia {index} de {relative}: {statement}"
                ) from exc
