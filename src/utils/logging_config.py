#############################################
# Name: logging_config.py
# Author: Christopher O. Romanillos
# Description: Centralized logging configuration for
#   Python scripts with JSON-formatted output,
#   stdout/stderr streaming, and environment-aware
#   settings to support Airflow, Docker, and
#   Fluent Bit integration.
# Date: 08/11/25
#############################################

import os
import logging
import logging.config
import yaml
from pathlib import Path
from typing import Optional

_cached_logging_config = None


def setup_logging(config_path: Optional[str] = None, logger_name: Optional[str] = None) -> logging.Logger:
    """
    Load and apply logging config from a YAML file once per process.
    Detects config file based on APP_ENV if config_path not provided.
    Returns a named logger for dependency injection or root logger by default.
    """
    global _cached_logging_config

    project_root = Path(__file__).resolve().parent.parent.parent
    config_dir = project_root / "config"

    if config_path is None:
        env = os.getenv("APP_ENV", "prod").lower()
        if env == "test":
            config_path = config_dir / "config-test.yaml"
        elif env == "dev":
            config_path = config_dir / "config-dev.yaml"
        else:
            config_path = config_dir / "config.yaml"
    else:
        config_path = Path(config_path)

    if _cached_logging_config is None:
        if not config_path.is_file():
            raise FileNotFoundError(f"Logging config file not found: {config_path}")

        with open(config_path, "r") as f:
            full_config = yaml.safe_load(f)

        _cached_logging_config = full_config.get("logging", {})

    if _cached_logging_config:
        logging.config.dictConfig(_cached_logging_config)
    else:
        logging.basicConfig(level=logging.INFO)

    return logging.getLogger(logger_name) if logger_name else logging.getLogger()
