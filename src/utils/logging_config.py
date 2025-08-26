#############################################
# Name: logging_config.py
# Author: Christopher O. Romanillos
# Description: Centralized logging configuration for
#   Python scripts with JSON-formatted output,
#   stdout/stderr streaming, and environment-agnostic
#   setup (relies on config_loader for APP_ENV).
# Date: 08/25/25
#############################################

import sys
import logging
import logging.config
from typing import Optional, Dict
from pythonjsonlogger import jsonlogger


###########################################
# Fallback Logger
###########################################
def create_fallback_logger(level: int = logging.INFO) -> logging.Logger:
    """
    Create a simple JSON logger to stdout/stderr
    for when no logging config is provided.
    Always attaches to the root logger.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return root_logger  # already configured

    # Shared formatter
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    # stdout: everything at 'level' and above
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)
    stdout_handler.setFormatter(formatter)

    # stderr: warnings and above
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(formatter)

    root_logger.setLevel(level)
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(stderr_handler)

    return root_logger


###########################################
# Centralized Logging Configuration Utility    
###########################################
def configure_logger(
    logging_config: Optional[Dict] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Configure the root logger from a provided logging config dict.
    Falls back to JSON stdout/stderr logging if no config is passed.

    Args:
        logging_config: A dictionary compliant with logging.config.dictConfig
        level: Default log level if no config is provided

    Returns:
        The root logger instance
    """
    root_logger = logging.getLogger()

    # Prevent duplicate handlers on re-initialization
    if root_logger.handlers:
        return root_logger

    if logging_config:
        logging.config.dictConfig(logging_config)
    else:
        create_fallback_logger(level)

    return root_logger
