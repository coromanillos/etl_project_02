#############################################
# Name: logging_config.py
# Author: Christopher O. Romanillos
# Description: Centralized logging configuration for
#   Python scripts with JSON-formatted output,
#   stdout/stderr streaming, and environment-agnostic
#   setup (relies on config_loader for APP_ENV).
# Date: 08/18/25
#############################################

import sys
import logging
import logging.config
from typing import Optional, Dict
from pythonjsonlogger import jsonlogger

"""
When Config_loader fails to provide a proper logging dict*
We alrady have config schema validation, that acts as the first line
of defense, and if the schema validation fails, it already strop the
project from starting by gracefully throwing a critical error and 
stopping the project from executing? If it does that, wouldn't this script
never be run? Bottom line, is it not made obsolete by config_schema?
Only viable use case is as a failsafe, but realistically should probably
focus more on ensuring validation is foolproof. 
"""
def create_fallback_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Create a simple JSON logger to stdout/stderr
    for when no logging config is provided.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
    ))

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
    ))

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    logger.setLevel(level)
    return logger


def configure_logger(
    name: str = __name__,
    logging_config: Optional[Dict] = None,
    level: int = logging.INFO
) -> logging.Logger:
    """
    Configure a logger from a provided logging config dict.
    Falls back to JSON stdout/stderr logging if no config is passed.
    """
    if logging_config:
        logging.config.dictConfig(logging_config)
        return logging.getLogger(name)
    return create_fallback_logger(name, level)
