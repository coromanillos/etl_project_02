#####################################################################
# Name: wrapper_etl_streaming.py
# Author: Christopher O. Romanillos (Refactored by ChatGPT)
# Description: Wrapper for streaming ETL (extract + transform per page)
# Date: 08/13/25
#####################################################################

import logging
from src.usgs_monitoring_locations.extract_usgs_monitoring_locations import extract_usgs_monitoring_locations_stream
from src.usgs_monitoring_locations.transform_usgs_monitoring_locations import transform_and_serialize_page
from src.usgs_monitoring_locations.load_usgs_monitoring_locations import load_usgs_monitoring_locations
from utils.db_config import get_engine
from models.monitoring_schema import metadata

logger = logging.getLogger(__name__)

def run_streaming_etl(url: str, http_client, session_factory=get_engine, metadata_obj=metadata):
    """
    Extract, transform, and load USGS monitoring locations per page.

    Args:
        url: API endpoint
        http_client: HTTP client implementing .get()
        session_factory: DB session factory
        metadata_obj: SQLAlchemy metadata
    """
    logger.info("Starting streaming ETL")

    for page_df in extract_usgs_monitoring_locations_stream(url, http_client):
        if page_df.empty:
            continue

        # Transform and serialize page
        parquet_bytes = transform_and_serialize_page(page_df)

        # Convert back to DataFrame in-memory for loading
        import pandas as pd
        df_transformed = pd.read_parquet(pd.io.common.BytesIO(parquet_bytes))

        # Load into DB
        load_usgs_monitoring_locations(df_transformed, session_factory, metadata_obj)

        logger.info(f"Processed page with {len(df_transformed)} records")

    logger.info("Streaming ETL complete")
