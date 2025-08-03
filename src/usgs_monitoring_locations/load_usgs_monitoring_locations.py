###########################################
# Name: load_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Load data from USGS REST API 
# Endpoint: monitoring_locations
# Date: 08/03/25
###########################################

import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.monitoring_schema import Base, MonitoringLocation, monitoring_location_schema
from utils.db_config import get_postgres_url

logger = logging.getLogger(__name__)

def load_data_to_postgres(df: pd.DataFrame) -> None:
    """
    Loads cleaned and validated data into PostgreSQL using SQLAlchemy ORM.
    """
    # ✅ Double-check schema validity before loading
    try:
        monitoring_location_schema.validate(df)
    except Exception as e:
        logger.error(f"DataFrame failed validation before load: {e}")
        raise

    engine = create_engine(get_postgres_url())
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    with Session() as session:
        try:
            records = [MonitoringLocation(**row.to_dict()) for _, row in df.iterrows()]
            session.bulk_save_objects(records)
            session.commit()
            logger.info(f"Inserted {len(records)} records into PostgreSQL")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to load data into PostgreSQL: {e}")
            raise
