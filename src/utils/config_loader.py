#############################################
# Name: config_loader.py
# Author: Christopher O. Romanillos
# Description: Environment-aware configuration
#   loader for YAML-based configs. Centralizes
#   APP_ENV handling for CI/CD pipelines.
# Date: 08/18/25
#############################################

import os
import yaml
from pathlib import Path
from typing import Dict, Any


def get_env_config_path() -> Path:
    """
    Return the environment-specific YAML config path based on APP_ENV.
    Defaults to production if APP_ENV is unset.
    """
    config_dir = Path(__file__).resolve().parent.parent / "config"
    env = os.getenv("APP_ENV", "prod").lower()
    if env in ("dev", "test"):
        return config_dir / f"config-{env}.yaml"
    return config_dir / "config.yaml"


def load_yaml_config(path: Path) -> Dict[str, Any]:
    """
    Load full YAML configuration file into a dict.
    Raises FileNotFoundError if the path is invalid.
    """
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}
