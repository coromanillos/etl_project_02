###########################################
# Name: db_config.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema script (refactored for nested Config)
# Date: 08/03/25
###########################################

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from src.utils.config import load_config, Config

def get_engine(config: Config = None) -> Engine:
    if config is None:
        config = load_config("config/config.yaml")

    db_url = (
        f"postgresql://{config.db.user}:{config.db.password}"
        f"@{config.db.host}:{config.db.port}/{config.db.name}"
    )
    return create_engine(db_url)
