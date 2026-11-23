"""Copia los CSV originales desde la bandeja de entrada a Raw."""

from __future__ import annotations

from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from shutil import copyfile

from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)


def _checksum(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def ingest_files(inbox: Path, raw: Path, filenames: Sequence[str]) -> dict[str, int]:
    """Copia archivos faltantes y verifica que los existentes sean idénticos."""
    if not filenames or len(set(filenames)) != len(filenames):
        raise ValueError("La ingesta Raw requiere una lista de archivos sin duplicados")
    if any(Path(name).name != name or not name.endswith(".csv") for name in filenames):
        raise ValueError("La configuración Raw admite únicamente nombres de archivos CSV")

    pending = []
    for name in filenames:
        source = inbox / name
        target = raw / name
        if not source.is_file() or source.stat().st_size == 0:
            raise ValueError(f"Falta el CSV de origen o está vacío: {source}")

        expected = _checksum(source)
        if not target.exists():
            pending.append((source, target, expected))
        elif _checksum(target) != expected:
            raise ValueError(f"El archivo en Raw tiene contenido distinto al origen: {target}")
        else:
            LOGGER.info("Archivo ya disponible en Raw: %s", name)

    raw.mkdir(parents=True, exist_ok=True)
    for source, target, expected in pending:
        copyfile(source, target)
        if _checksum(target) != expected:
            raise IOError(f"La copia no coincide con el archivo de origen: {target}")
        LOGGER.info("Archivo copiado a Raw: %s", target.name)

    return {"copiados": len(pending), "existentes": len(filenames) - len(pending)}
