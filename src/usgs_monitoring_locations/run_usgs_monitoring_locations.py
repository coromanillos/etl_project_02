###########################################
# Name: run_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Wrapper script for DAG orchestration
# Date: 08/02/25
###########################################

import logging
import pandas as pd
from src.utils.config import load_config
from src.usgs_monitoring_locations.extract_usgs_monitoring_locations import extract_usgs_monitoring_locations
from src.usgs_monitoring_locations.transform_usgs_monitoring_locations import parse_usgs_monitoring_locations
from src.usgs_monitoring_locations.loading_usgs_monitoring_locations import load_data_to_postgres

logger = logging.getLogger(__name__)

def extract_task(**kwargs) -> dict:
    logger.info("Starting extract task")
    config = load_config()
    data = extract_usgs_monitoring_locations(config)
    if data is None:
        logger.error("Extract task failed: No data returned")
        raise ValueError("Extract task failed: No data returned.")
    logger.info("Extract task completed successfully")
    return data

def transform_task(raw_data: dict, **kwargs) -> list[dict]:
    logger.info("Starting transform task")
    df = parse_usgs_monitoring_locations(raw_data)
    if df is None or df.empty:
        logger.error("Transform task failed: No valid data")
        raise ValueError("Transform task failed: No valid data.")
    logger.info(f"Transform task completed successfully, {len(df)} records processed")
    return df.to_dict(orient="records")

def load_task(records: list[dict], **kwargs):
    logger.info("Starting load task")
    df = pd.DataFrame(records)
    load_data_to_postgres(df)
    logger.info("Load task completed successfully")
