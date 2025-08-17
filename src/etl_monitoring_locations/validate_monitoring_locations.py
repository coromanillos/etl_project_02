##############################################
# Name: validate_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Validate structure of data
# Date: 08/06/25
##############################################

import pandas as pd

REQUIRED_COLUMNS = ["site_number", "latitude", "longitude"]

def validate_usgs_monitoring_locations(df: pd.DataFrame) -> None:
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    if df[REQUIRED_COLUMNS].isnull().any().any():
        raise ValueError("Null values found in required columns")

    # Range check for coordinates
    if not df["latitude"].between(-90, 90).all():
        raise ValueError("Latitude values out of range (-90 to 90)")

    if not df["longitude"].between(-180, 180).all():
        raise ValueError("Longitude values out of range (-180 to 180)")
