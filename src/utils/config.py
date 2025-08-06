###########################################
# Name: config.py
# Author: Christopher O. Romanillos
# Description: Config utility loader
# Date: 08/02/25
###########################################

from pydantic_settings import BaseSettings
from functools import lru_cache

class Config(BaseSettings):
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    db_name: str
    monitoring_locations_url: str

    model_config = {
        "env_file": ".env",
        "extra": "ignore",
    }

@lru_cache()
def get_config():
    return Config()

