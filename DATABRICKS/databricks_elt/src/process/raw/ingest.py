"""Copia los CSV originales desde la bandeja de entrada a Raw."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from fnmatch import fnmatchcase
from hashlib import sha256
from pathlib import Path
from shutil import copyfile

from utils.logging_config import get_logger

LOGGER = get_logger(__name__)


def _checksum(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def ingest_files(
    inbox: Path, raw: Path, routes: Sequence[Mapping[str, str]]
) -> dict[str, int]:
    """Clasifica cada CSV y comprueba conflictos antes de publicar."""
    if not routes:
        raise ValueError("Faltan rutas de destino para los CSV")
    directories = [route["subdirectory"] for route in routes]
    if len(set(directories)) != len(directories) or any(
        not directory or directory in {".", ".."} or Path(directory).name != directory
        for directory in directories
    ):
        raise ValueError("Las subcarpetas Raw deben ser nombres únicos")
    inbox.mkdir(parents=True, exist_ok=True)
    pending = []
    existing = 0
    for source in sorted(inbox.iterdir()):
        if not source.is_file() or source.suffix != ".csv":
            continue
        name = source.name
        matches = [
            route["subdirectory"] for route in routes
            if fnmatchcase(name, route["source_file_pattern"])
        ]
        if not matches:
            LOGGER.warning("CSV sin ruta configurada: %s", name)
            continue
        if len(matches) != 1:
            raise ValueError(f"Más de una ruta coincide con {name}")
        if source.stat().st_size == 0:
            raise ValueError(f"El CSV está vacío: {source}")

        target = raw / matches[0] / name

        expected = _checksum(source)
        if not target.exists():
            pending.append((source, target, expected))
        elif _checksum(target) != expected:
            raise ValueError(f"El archivo en Raw tiene contenido distinto al origen: {target}")
        else:
            existing += 1
            LOGGER.info("Archivo ya disponible en Raw: %s", name)

    for directory in directories:
        (raw / directory).mkdir(parents=True, exist_ok=True)
    for source, target, expected in pending:
        copyfile(source, target)
        if _checksum(target) != expected:
            raise IOError(f"La copia no coincide con el archivo de origen: {target}")
        LOGGER.info("Archivo copiado a Raw: %s/%s", target.parent.name, target.name)

    return {"copiados": len(pending), "existentes": existing}
