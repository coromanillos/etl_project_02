###########################################
# Name: transform_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos (Refactored by ChatGPT)
# Description: Transform USGS monitoring location data per page for streaming ETL
# Date: 08/13/25
###########################################

import logging
import pandas as pd
import io

logger = logging.getLogger(__name__)

RENAME_MAP = {
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

def transform_usgs_monitoring_locations_page(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a single page of monitoring location data.

    Args:
        df: Raw page DataFrame from extractor

    Returns:
        Transformed DataFrame
    """
    if df.empty:
        return df

    df.rename(columns=RENAME_MAP, inplace=True)

    for col in ["latitude", "longitude", "elevation_ft"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "inventory_date" in df.columns:
        df["inventory_date"] = pd.to_datetime(df["inventory_date"], errors="coerce")

    return df

def transform_and_serialize_page(df: pd.DataFrame) -> bytes:
    """
    Transform and serialize DataFrame to Parquet bytes for upload.

    Args:
        df: Raw DataFrame

    Returns:
        Bytes of Parquet file
    """
    transformed_df = transform_usgs_monitoring_locations_page(df)
    parquet_buffer = io.BytesIO()
    transformed_df.to_parquet(parquet_buffer, index=False)
    return parquet_buffer.getvalue()
