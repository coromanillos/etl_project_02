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
    config = load_config()
    data = extract_usgs_monitoring_locations(config)
    if data is None:
        raise ValueError("Extract task failed: No data returned.")
    return data  # ✅ Serializable

def transform_task(raw_data: dict, **kwargs) -> list[dict]:
    df = parse_usgs_monitoring_locations(raw_data)
    if df is None or df.empty:
        raise ValueError("Transform task failed: No valid data.")
    return df.to_dict(orient="records")  # ✅ JSON-serializable

def load_task(records: list[dict], **kwargs):
    df = pd.DataFrame(records)
    load_data_to_postgres(df)
    logger.info("Load task completed.")
