"""Carga y valida la configuración YAML del proyecto."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)


def load_yaml(path: Path) -> dict[str, Any]:
    """Lee un archivo YAML y avisa si no contiene una configuración válida."""
    try:
        with path.open(encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
    except OSError:
        LOGGER.exception("No se pudo leer el archivo de configuración: %s", path)
        raise
    except yaml.YAMLError:
        LOGGER.exception("El archivo de configuración no contiene YAML válido: %s", path)
        raise

    if not isinstance(config, dict):
        raise ValueError(f"El archivo debe contener un objeto de configuración YAML: {path}")
    return config
