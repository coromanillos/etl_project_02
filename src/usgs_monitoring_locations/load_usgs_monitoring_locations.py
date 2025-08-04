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
from utils.serialization import deserialize_df

logger = logging.getLogger(__name__)

def load_usgs_monitoring_locations(parquet_blob: bytes) -> None:
    try:
        df = deserialize_df(parquet_blob)

        monitoring_location_schema.validate(df)

        engine = create_engine(get_postgres_url())
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        with Session() as session:
            records = [MonitoringLocation(**row.to_dict()) for _, row in df.iterrows()]
            session.bulk_save_objects(records)
            session.commit()

        logger.info(f"Inserted {len(records)} records into PostgreSQL")

    except Exception as e:
        logger.error(f"Failed to load monitoring locations: {e}")
        raise
