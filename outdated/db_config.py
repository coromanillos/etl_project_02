###########################################
# Name: db_config.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema script (refactored for nested Config)
# Date: 08/03/25
# I hardcoded db connection for the most part since I dont do it often, but look into it I guess...
###########################################

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
from src.utils.config import load_config, Config
import logging

etLogger(__name__)

def get_engine(config: Config = None, config_path: str = "config/config.yaml") -> Engine:
    if config is None:
        config = load_config(config_path)

    # URL-encode user and password for safety
    user = quote_plus(config.db.user)
    password = quote_plus(config.db.password)
    host = config.db.host
    port = config.db.port
    database = config.db.database

    db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    logger.info(f"Creating DB engine with URL: postgresql://{user}:***@{host}:{port}/{database}")
    engine = create_engine(db_url)
    return engine
