###########################################
# Name: db_config.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema scipt 
# Date: 08/03/25
###########################################

import os
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

def get_postgres_url():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", "postgres"))
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "monitoring")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"
