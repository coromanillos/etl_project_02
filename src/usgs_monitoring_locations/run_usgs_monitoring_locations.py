###########################################
# Name: run_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Wrapper script for DAG orchestration (refactored for nested Config)
# Date: 08/02/25
###########################################

import logging
import pandas as pd
from src.usgs_monitoring_locations.extract_usgs_monitoring_locations import extract_usgs_monitoring_locations
from src.usgs_monitoring_locations.transform_usgs_monitoring_locations import transform_usgs_monitoring_locations
from src.usgs_monitoring_locations.load_usgs_monitoring_locations import load_usgs_monitoring_locations
from src.utils.config import load_config, Config
from src.common.http_client import RequestsHttpClient

logger = logging.getLogger(__name__)

def extract_task(url: str, http_client) -> pd.DataFrame:
    logger.info("Starting extract task")
    df = extract_usgs_monitoring_locations(url, http_client)
    if df is None or df.empty:
        raise ValueError("Extract task failed: No data returned.")
    logger.info(f"Extract task completed successfully with {len(df)} records")
    return df

def transform_task(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    logger.info("Starting transform task")
    parquet_bytes = transform_usgs_monitoring_locations(df.to_dict(orient="records"), cfg)
    if not parquet_bytes:
        raise ValueError("Transform task failed: No valid data.")
    transformed_df = pd.read_parquet(pd.io.common.BytesIO(parquet_bytes))
    logger.info(f"Transform task completed successfully, {len(transformed_df)} records processed")
    return transformed_df

def load_task(df: pd.DataFrame, session_factory, metadata):
    logger.info("Starting load task")
    load_usgs_monitoring_locations(df, session_factory, metadata)
    logger.info("Load task completed successfully")

if __name__ == "__main__":
    config = load_config("config/config.yaml")
    http_client = RequestsHttpClient()

    url = config.usgs.monitoring_locations_url

    df_extracted = extract_task(url, http_client)
    df_transformed = transform_task(df_extracted, config)
    load_task(df_transformed, session_factory=..., metadata=...)  # Fill in your DB session and metadata
