#####################################################################
# Name: config_loader.py
# Author: Christopher O. Romanillos
# Description: Loads and validates environment-specific YAML configs
# Date: 08/18/25
#####################################################################

import os
import yaml
from pathlib import Path
from typing import Any, Dict
from dotenv import load_dotenv

from config_schema import get_config_schema

# Load environment variables from .env if present using APP_ENV
load_dotenv()


def get_env_config_path() -> Path:
    """
    Selects the config file path based on APP_ENV.
    Defaults to production config.yaml.
    """
    project_root = Path(__file__).resolve().parent.parent
    config_dir = project_root / "config"

    env = os.getenv("APP_ENV", "prod").lower()
    config_map = {
        "dev": config_dir / "config-dev.yaml",
        "test": config_dir / "config-test.yaml",
        "prod": config_dir / "config.yaml",
    }

    if env not in config_map:
        raise ValueError(f"Invalid APP_ENV '{env}'. Must be one of {list(config_map.keys())}.")

    return config_map[env]


def validate_config(config: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """
    Validates a config dictionary against the provided schema.
    Raises ValueError if any required key is missing or type mismatched.
    """
    for key, expected in schema.items():
        if key not in config:
            raise ValueError(f"Missing required section: '{key}'")

        if isinstance(expected, dict):
            if not isinstance(config[key], dict):
                raise ValueError(f"Section '{key}' must be a dict.")
            validate_config(config[key], expected)
        else:
            if not isinstance(config[key], expected):
                raise ValueError(f"Key '{key}' must be of type {expected.__name__}, "
                                 f"got {type(config[key]).__name__} instead.")

# -----------------------------
# Main config method
# -----------------------------
def load_config() -> Dict[str, Any]:
    """
    Loads and validates YAML config based on APP_ENV.
    """
    config_path = get_env_config_path()

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    schema = get_config_schema()
    validate_config(config, schema)

    return config
