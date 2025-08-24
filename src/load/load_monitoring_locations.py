###########################################
# Name: load_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Load data from USGS REST API 
# Endpoint: monitoring_locations
# Date: 08/03/25
###########################################

import logging
import pandas as pd
from sqlalchemy.engine import Engine
from src.usgs_monitoring_locations.validate_usgs_monitoring_locations import validate_usgs_monitoring_locations

logger = logging.getLogger(__name__)

def load_usgs_monitoring_locations(
    df: pd.DataFrame,
    session_factory: callable,
    metadata: callable
) -> None:
    try:
        validate_usgs_monitoring_locations(df)

        engine: Engine = session_factory()
        metadata(engine)

        df.to_sql(
            name="monitoring_locations",
            con=engine,
            index=False,
            if_exists="append",
            method="multi"
        )
        logger.info(f"Inserted {len(df)} records into PostgreSQL")

    except Exception as e:
        logger.exception("Failed to load monitoring locations")
        raise
