###########################################
# Name: run_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Wrapper script for DAG orchestration
# Date: 08/02/25
###########################################

import logging
import pandas as pd
from src.usgs_monitoring_locations.extract_usgs_monitoring_locations import extract_usgs_monitoring_locations
from src.usgs_monitoring_locations.transform_usgs_monitoring_locations import transform_usgs_monitoring_locations
from src.usgs_monitoring_locations.load_usgs_monitoring_locations import load_usgs_monitoring_locations

logger = logging.getLogger(__name__)

def extract_task(config: dict, http_client) -> dict:
    logger.info("Starting extract task")
    data = extract_usgs_monitoring_locations(config, http_client)
    if not data:
        raise ValueError("Extract task failed: No data returned.")
    logger.info("Extract task completed successfully")
    return data

def transform_task(raw_data: dict) -> list[dict]:
    logger.info("Starting transform task")
    parquet_bytes = transform_usgs_monitoring_locations(raw_data)
    if not parquet_bytes:
        raise ValueError("Transform task failed: No valid data.")
    df = pd.read_parquet(pd.io.common.BytesIO(parquet_bytes))
    logger.info(f"Transform task completed successfully, {len(df)} records processed")
    return df.to_dict(orient="records")

def load_task(records: list[dict], session_factory, metadata):
    logger.info("Starting load task")
    df = pd.DataFrame(records)
    parquet_blob = df.to_parquet(index=False)
    load_usgs_monitoring_locations(parquet_blob, session_factory, metadata)
    logger.info("Load task completed successfully")