###########################################################################
# Name: transform_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Transform/Flatten + Clean USGS monitoring locations raw data
# Date: 08/03/25
###########################################################################

import logging
import pandas as pd
from typing import Optional
from src.utils.config import load_config
from src.utils.serialization import save_dataframe_to_parquet

logger = logging.getLogger(__name__)

def transform_usgs_monitoring_locations(cfg: Optional[dict] = None) -> Optional[pd.DataFrame]:
    """
    Transform and clean raw USGS monitoring location data.

    Args:
        cfg: Configuration dictionary with paths and settings.

    Returns:
        pd.DataFrame or None: Transformed dataframe if successful, else None.
    """
    if cfg is None:
        cfg = load_config("config/config.yaml")  # Provide actual path here

    try:
        raw_path = cfg["output"]["directory"] + "/" + cfg["output"]["filename_pattern"].format(timestamp="*")
        # Or adjust path acquisition logic as needed, e.g., cfg["data_paths"]["raw"]
        df = pd.read_parquet(raw_path)
        logger.info(f"Read {len(df)} raw records from {raw_path}")

        rename_map = {
            "dec_lat_va": "latitude",
            "dec_long_va": "longitude",
            "alt_va": "elevation_ft",
            "state_cd": "state_code",
            "county_cd": "county_code",
            "huc_cd": "huc_code",
            "agency_cd": "agency_code",
            "site_no": "site_number",
            "station_nm": "station_name",
            "inventory_dt": "inventory_date"
        }
        df.rename(columns=rename_map, inplace=True)

        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["elevation_ft"] = pd.to_numeric(df["elevation_ft"], errors="coerce")
        df["inventory_date"] = pd.to_datetime(df["inventory_date"], errors="coerce")

        logger.info(f"Transformed data: {df.shape[0]} records, {df.shape[1]} columns")
        return df

    except Exception as e:
        logger.error(f"Failed to transform USGS monitoring locations: {e}", exc_info=True)
        return None
