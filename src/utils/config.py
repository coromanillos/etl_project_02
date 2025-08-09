#############################################
# Name: config.py
# Author: Christopher O. Romanillos
# Description: Central point of config access with
#   schema validation, explicit .env loading,
#   secrets separation, and robust merging.
# Date: 08/02/25
#############################################

import os
import yaml
import logging
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field, ValidationError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Explicitly load .env file if it exists
env_path = Path(".env")
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info(f"Loaded environment variables from {env_path}")

def deep_merge(d1: dict, d2: dict) -> dict:
    """
    Recursively merge d2 into d1, returning a new dict.
    """
    result = dict(d1)
    for k, v in d2.items():
        if (
            k in result
            and isinstance(result[k], dict)
            and isinstance(v, dict)
        ):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result

class Config(BaseSettings):
    # USGS API
    monitoring_locations_url: str = Field(
        "https://mock-url.com", env="API_URL"
    )
    
    # Output settings
    output_directory: str = Field("mock_data", env="OUTPUT_DIR")
    filename_pattern: str = Field(
        "usgs_monitoring_locations_{timestamp}.json", env="FILENAME_PATTERN"
    )
    timestamp_format: str = Field("%Y%m%d_%H%M%S", env="TIMESTAMP_FORMAT")

    # Secrets (kept only in env vars)
    api_key: Optional[str] = Field(None, env="API_KEY")

    class Config:
        # Allow loading from env and dotenv automatically
        env_file = ".env"
        env_file_encoding = "utf-8"

def load_yaml_config(path: Optional[str]) -> dict:
    if path is None:
        return {}
    try:
        with open(path, "r") as f:
            config = yaml.safe_load(f) or {}
            logger.info(f"Loaded config YAML from {path}")
            return config
    except FileNotFoundError:
        logger.warning(f"Config file not found at {path}, skipping YAML config.")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML config at {path}: {e}")
        return {}

def load_config(path: Optional[str] = None) -> Config:
    """
    Load config from YAML file merged with environment variables,
    with validation and secrets separation.

    Args:
        path (str, optional): Path to YAML config file.

    Returns:
        Config: Validated config dataclass.
    """
    yaml_config = load_yaml_config(path)
    # Normalize keys to match Config fields (flatten nested dict)
    flat_config = {
        "monitoring_locations_url": yaml_config.get("usgs", {}).get("monitoring_locations_url"),
        "output_directory": yaml_config.get("output", {}).get("directory"),
        "filename_pattern": yaml_config.get("output", {}).get("filename_pattern"),
        "timestamp_format": yaml_config.get("output", {}).get("timestamp_format"),
    }
    # Remove None keys to allow env vars to override
    flat_config = {k: v for k, v in flat_config.items() if v is not None}

    try:
        config = Config(**flat_config)  # pydantic merges env vars automatically
        logger.info("Configuration loaded and validated successfully.")
        return config
    except ValidationError as ve:
        logger.error(f"Configuration validation error: {ve}")
        raise

