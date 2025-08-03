###########################################
# Name: loading_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Load data from USGS REST API 
# Endpoint: monitoring_locations
# Date: 08/03/25
###########################################

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.monitoring_schema import Base, MonitoringLocation
from utils.db_config import get_postgres_url

def load_data_to_postgres(df: pd.DataFrame) -> None:
    """
    Loads cleaned and validated data from a DataFrame into PostgreSQL
    using SQLAlchemy ORM.

    Args:
        df (pd.DataFrame): Cleaned, schema-conformant DataFrame.
    """
    engine = create_engine(get_postgres_url())
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    # Using context manager for session
    with Session() as session:
        try:
            # Convert each row to a dictionary for ORM instantiation
            records = [
                MonitoringLocation(**(row._asdict() if hasattr(row, '_asdict') else row.to_dict()))
                for _, row in df.iterrows()
            ]
            session.bulk_save_objects(records)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"[ERROR] Failed to load data into PostgreSQL: {e}")
            raise
