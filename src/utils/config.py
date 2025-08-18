#####################################################################
# Name: config.py
# Author: Christopher O. Romanillos
# Description: Central config loader using Pydantic with YAML
#   Environment-aware and CI/CD ready
# Date: 08/17/25
#####################################################################

import os
import logging
import logging.config
from pathlib import Path
from typing import Optional, Dict, Any
import yaml
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ----------------------
# Load environment variables
# ----------------------
env_path = Path(".env")
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info(f"Loaded environment variables from {env_path}")

# ----------------------
# Pydantic Config Models
# ----------------------
class DBConfig(BaseModel):
    user: str
    password: str
    host: str
    port: int
    database: str

class MonitoringLocationsConfig(BaseModel):
    base_url: str
    limit: int = 10000
    request_timeout: int = 10
    max_retries: int = 3
    timestamp_format: str = "%Y%m%d_%H%M%S"

class USGSConfig(BaseModel):
    monitoring_locations: MonitoringLocationsConfig

class PathsConfig(BaseModel):
    raw_data: str
    prepared_data: str

class Config(BaseModel):
    paths: PathsConfig
    usgs: USGSConfig
    db: DBConfig
    logging: Optional[Dict[str, Any]] = None  # Include logging dict for dictConfig

# ----------------------
# YAML Loader & Normalization
# ----------------------
def load_yaml_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    try:
        with open(path, "r") as f:
            cfg = yaml.safe_load(f) or {}
            logger.info(f"Loaded YAML config from {path}")
            # Normalize keys for Pydantic
            if "usgs" in cfg and "monitoring-locations" in cfg["usgs"]:
                cfg["usgs"]["monitoring_locations"] = cfg["usgs"].pop("monitoring-locations")
            return cfg
    except FileNotFoundError:
        logger.warning(f"Config file not found at {path}, skipping.")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML config at {path}: {e}")
        return {}

# ----------------------
# Load & validate config
# ----------------------
def load_config(path: Optional[str] = None) -> Config:
    # Environment-aware YAML selection
    if not path:
        env = os.getenv("APP_ENV", "prod").lower()
        project_root = Path(__file__).resolve().parent.parent
        config_dir = project_root / "config"
        path = config_dir / f"config-{env}.yaml" if env in ("dev", "test") else config_dir / "config.yaml"

    yaml_cfg = load_yaml_config(path)

    # Apply environment variable overrides
    if os.getenv("RAW_DATA_PATH"):
        yaml_cfg.setdefault("paths", {})["raw_data"] = os.getenv("RAW_DATA_PATH")
    if os.getenv("API_URL"):
        yaml_cfg.setdefault("usgs", {}).setdefault("monitoring_locations", {})["base_url"] = os.getenv("API_URL")
    if os.getenv("DB_USER"):
        yaml_cfg.setdefault("db", {})["user"] = os.getenv("DB_USER")
    if os.getenv("DB_PASSWORD"):
        yaml_cfg.setdefault("db", {})["password"] = os.getenv("DB_PASSWORD")

    try:
        cfg = Config(**yaml_cfg)
        logger.info("Configuration validated successfully.")

        # Initialize logging immediately if logging config exists
        if cfg.logging:
            logging.config.dictConfig(cfg.logging)
            logger.info("Logging initialized from config.yaml")
        return cfg
    except ValidationError as ve:
        logger.error(f"Configuration validation error: {ve}")
        raise
