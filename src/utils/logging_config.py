#############################################
# Name: logging_config.py
# Author: Christopher O. Romanillos
# Description: Centralized logging configuration for
#   Python scripts with JSON-formatted output,
#   stdout/stderr streaming, environment-aware,
#   and enriched logs for Airflow, Docker, and
#   Fluent Bit/ELK integration.
# Date: 08/17/25
#############################################

import os
import sys
import logging
import logging.config
import yaml
from pathlib import Path
from typing import Optional

_cached_logging_config = None

def setup_logging(
    config_path: Optional[str] = None,
    logger_name: Optional[str] = None,
    extra_context: Optional[dict] = None
) -> logging.Logger:
    """
    Load and apply logging config from a YAML file once per process.
    Detects config file based on APP_ENV if config_path not provided.
    Returns a named logger for dependency injection or root logger by default.
    
    extra_context: Optional dict to enrich log records with additional metadata
                   (e.g., service_name, task_id, run_id) for observability.
    """
    global _cached_logging_config

    project_root = Path(__file__).resolve().parent.parent.parent
    config_dir = project_root / "config"

    # Determine YAML logging configuration path based on environment
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

    # Load YAML logging configuration once
    if _cached_logging_config is None:
        if not config_path.is_file():
            raise FileNotFoundError(f"Logging config file not found: {config_path}")

        with open(config_path, "r") as f:
            full_config = yaml.safe_load(f)

        _cached_logging_config = full_config.get("logging", {})

    # Apply YAML logging configuration or fallback to default JSON stdout
    if _cached_logging_config:
        logging.config.dictConfig(_cached_logging_config)
    else:
        # Default JSON formatter for stdout/stderr
        json_formatter = {
            "format": '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}',
            "datefmt": "%Y-%m-%dT%H:%M:%S"
        }
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(logging.Formatter(**json_formatter))
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(logging.Formatter(**json_formatter))
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(stdout_handler)
        root_logger.addHandler(stderr_handler)

    logger = logging.getLogger(logger_name) if logger_name else logging.getLogger()

    # Optionally add extra context to all log records
    if extra_context:
        try:
            from pythonjsonlogger import jsonlogger
            for handler in logger.handlers:
                if isinstance(handler.formatter, jsonlogger.JsonFormatter):
                    handler.formatter.extra = extra_context
        except ImportError:
            logger.warning("python-json-logger not installed; extra context will not be added")

    return logger
