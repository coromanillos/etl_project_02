###########################################
# Name: db_config.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema script
# Date: 08/03/25
###########################################

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from src.utils.config import Config

def get_engine(config: Config) -> Engine:
    db_url = (
        f"postgresql://{config.postgres_user}:{config.postgres_password}"
        f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_db}"
    )
    return create_engine(db_url)
