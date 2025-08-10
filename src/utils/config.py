#############################################
# Name: config.py
# Author: Christopher O. Romanillos
# Description: Central point of config access with
#   schema validation, explicit .env loading,
#   secrets separation, and robust merging.
# Date: 08/02/25
#############################################

import logging
from pathlib import Path
from typing import Optional, Any, Dict
import yaml
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env explicitly once (could move to main app entry point)
env_path = Path(".env")
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info(f"Loaded environment variables from {env_path}")

class DBConfig(BaseModel):
    user: str
    password: str
    host: str
    port: int
    database: str

class Config(BaseSettings):
    monitoring_locations_url: str = Field(..., env="API_URL")
    output_directory: str = Field(..., env="OUTPUT_DIR")
    filename_pattern: str = Field(..., env="FILENAME_PATTERN")
    timestamp_format: str = Field("%Y%m%d_%H%M%S", env="TIMESTAMP_FORMAT")
    db: DBConfig

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"

def load_yaml_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}

    try:
        with open(path, "r") as f:
            config = yaml.safe_load(f) or {}
            logger.info(f"Loaded YAML config from {path}")
            return config
    except FileNotFoundError:
        logger.warning(f"Config file not found at {path}, skipping.")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML config at {path}: {e}")
        return {}

def load_config(path: Optional[str] = None) -> Config:
    yaml_config = load_yaml_config(path)
    merged_config = {}

    # Use `.get()` with defaults to avoid None if keys missing
    usgs_cfg = yaml_config.get("usgs", {})
    merged_config["monitoring_locations_url"] = usgs_cfg.get("monitoring_locations_url")

    output_cfg = yaml_config.get("output", {})
    merged_config["output_directory"] = output_cfg.get("directory")
    merged_config["filename_pattern"] = output_cfg.get("filename_pattern")
    merged_config["timestamp_format"] = output_cfg.get("timestamp_format")

    db_cfg = yaml_config.get("db")
    if db_cfg:
        merged_config["db"] = db_cfg

    try:
        # Filter out None values to allow env vars to fill gaps
        filtered_config = {k: v for k, v in merged_config.items() if v is not None}
        config = Config(**filtered_config)
        logger.info("Configuration loaded and validated successfully.")
        return config
    except ValidationError as ve:
        logger.error(f"Configuration validation error: {ve}")
        raise
