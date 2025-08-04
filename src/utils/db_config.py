###########################################
# Name: db_config.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema scipt 
# Date: 08/03/25
###########################################

import os
import logging
import urllib.parse
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

def get_postgres_url() -> str:
    """
    Constructs the PostgreSQL connection string using environment variables.
    """
    try:
        user = os.getenv("POSTGRES_USER", "postgres")
        password = urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", "postgres"))
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "monitoring")

        url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        logger.debug(f"Postgres URL constructed successfully.")
        return url
    except Exception as e:
        logger.error(f"Error constructing Postgres URL: {e}")
        raise
