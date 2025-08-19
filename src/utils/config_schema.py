#####################################################################
# Name: config_schema.py
# Author: Christopher O. Romanillos
# Description: Defines schema for validating YAML configuration files
# Date: 08/18/25
#####################################################################

from typing import Dict, Any


def get_config_schema() -> Dict[str, Any]:
    """
    Returns the expected schema for application configuration.
    Extend this dictionary as your configs evolve.
    This is probably a really big issue for maintainability...
    It will most likely have to grow alongside your production dev and test configs...
    I want to try APP_ENV standardization to see if integration with CI/CD
    really is worth it... 08/19/25
    """
    return {
        "database": {
            "host": str,
            "port": int,
            "user": str,
            "password": str,
            "name": str,
        },
        "logging": {
            "level": str,
            "format": str,
        },
        "api": {
            "base_url": str,
            "timeout": int,
        },
    }
