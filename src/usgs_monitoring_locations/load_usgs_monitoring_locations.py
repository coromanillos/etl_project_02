###########################################
# Name: load_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Load data from USGS REST API 
# Endpoint: monitoring_locations
# Date: 08/03/25
###########################################

import logging
import pandas as pd
from sqlalchemy.orm import Session as SessionType
from models.monitoring_schema import MonitoringLocation, monitoring_location_schema
from utils.serialization import deserialize_df

logger = logging.getLogger(__name__)

def load_usgs_monitoring_locations(parquet_blob: bytes, session_factory: callable, metadata: callable) -> None:
    try:
        df = deserialize_df(parquet_blob)
        monitoring_location_schema.validate(df)

        engine = session_factory()
        metadata(engine)
        session = engine()

        with session as s:
            records = [MonitoringLocation(**row.to_dict()) for _, row in df.iterrows()]
            s.bulk_save_objects(records)
            s.commit()

        logger.info(f"Inserted {len(records)} records into PostgreSQL")

    except Exception as e:
        logger.exception("Failed to load monitoring locations")
        raise