###########################################
# Name: config.py
# Author: Christopher O. Romanillos
# Description: Config utility loader
# Date: 08/02/25
###########################################

import yaml
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

def load_config(config_path: Optional[str] = None) -> dict:
    """
    Load YAML config from file.

    Path can be overridden by environment variable CONFIG_PATH.
    """
    config_path = config_path or os.getenv("CONFIG_PATH", "config.yaml")
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logger.info(f"Config loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing config file: {e}")
        raise
