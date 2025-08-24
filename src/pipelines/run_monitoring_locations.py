#####################################################################
# Name: run_monitoring_locations.py
# Author: Christopher O. Romanillos 
# Description: Entrypoint for monitoring locations ETL 
# Date: 08/16/25
#####################################################################

from utils.config import load_config
from src.logging_config import configure_logger
from src.etl_monitoring_locations.extract_monitoring_locations import extract_all_monitoring_locations
from src.etl_monitoring_locations.transform_usgs_monitoring_locations import transform_and_serialize_page
from src.etl_monitoring_locations.load_usgs_monitoring_locations import load_usgs_monitoring_locations
from utils.db_config import get_engine
from models.monitoring_schema import metadata

# --------------------------
# Load config & logging once
# --------------------------
config = load_config("config/config.yaml")
logger = configure_logger(__name__, logging_config=config.get("logging"))
logger.info("Wrapper loaded configuration successfully")

def run_streaming_etl(http_client=None):
    """Extract, transform, and load USGS monitoring locations."""
    logger.info("Starting streaming ETL")

    # Step 1: Extract all data to raw_data
    extract_all_monitoring_locations(config, logger=logger, http_client=http_client)

    # Step 2: Transform & load (streaming by page)
    ml_cfg = config["usgs"]["monitoring_locations"]
    url = ml_cfg["base_url"]

    from src.usgs_monitoring_locations.extract_monitoring_locations_stream import extract_monitoring_locations_stream
    for page_df in extract_monitoring_locations_stream(url, http_client, ml_cfg["limit"]):
        if page_df.empty:
            continue

        parquet_bytes = transform_and_serialize_page(page_df)
        import pandas as pd
        df_transformed = pd.read_parquet(pd.io.common.BytesIO(parquet_bytes))

        load_usgs_monitoring_locations(df_transformed, get_engine, metadata)
        logger.info(f"Processed page with {len(df_transformed)} records")

    logger.info("Streaming ETL complete")
