###########################################
# Name: db_config.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema script
# Date: 08/03/25
###########################################

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from src.utils.config import load_config

def get_engine(config: dict = None) -> Engine:
    if config is None:
        config = load_config("config/config.yaml")  # Provide actual path here

    db = config["db"]
    db_url = (
        f"postgresql://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['name']}"
    )
    return create_engine(db_url)
