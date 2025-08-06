###########################################
# Name: config.py
# Author: Christopher O. Romanillos
# Description: Config utility loader
# Date: 08/02/25
###########################################

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="forbid")

    monitoring_locations_url: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int
    postgres_db: str


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


config = Config()
