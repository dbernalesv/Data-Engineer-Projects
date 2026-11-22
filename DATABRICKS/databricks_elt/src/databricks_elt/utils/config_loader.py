"""Load and validate YAML project configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from databricks_elt.utils.logging_config import get_logger

LOGGER = get_logger(__name__)


def load_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML mapping from disk and fail clearly for invalid config."""
    try:
        with path.open(encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
    except OSError:
        LOGGER.exception("Could not read configuration file %s", path)
        raise
    except yaml.YAMLError:
        LOGGER.exception("Invalid YAML in configuration file %s", path)
        raise

    if not isinstance(config, dict):
        raise ValueError(f"Configuration file must contain a YAML mapping: {path}")
    return config
