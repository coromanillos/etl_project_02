###################################################################
# Name: transform_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Transform/Flatten + Clean USGS monitoring locations raw data
# Date: 08/03/25
###################################################################

import logging
import pandas as pd
from utils.serialization import serialize_df
from typing import Optional

logger = logging.getLogger(__name__)

def transform_usgs_monitoring_locations(raw_data: dict) -> Optional[bytes]:
    try:
        features = raw_data.get("features", [])
        if not features:
            logger.warning("No features found in raw data")
            return None

        records = [
            {**feature.get("properties", {}), "id": feature.get("id")}
            for feature in features
        ]

        df = pd.DataFrame(records)

        logger.info(f"Parsed {len(df)} raw monitoring location records")

        df.rename(columns={
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
        }, inplace=True)

        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["elevation_ft"] = pd.to_numeric(df["elevation_ft"], errors="coerce")
        df["inventory_date"] = pd.to_datetime(df["inventory_date"], errors="coerce")

        df.dropna(subset=["latitude", "longitude", "site_number"], inplace=True)

        logger.info(f"Cleaned data to {len(df)} valid monitoring location records")

        return serialize_df(df)

    except Exception as e:
        logger.exception("Error transforming USGS monitoring locations")
        return None
