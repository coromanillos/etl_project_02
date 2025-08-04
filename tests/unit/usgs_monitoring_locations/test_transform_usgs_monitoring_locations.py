# test_usgs_transform_monitoring_locations.py 

import pytest
import pandas as pd
from src.transform_usgs_monitoring_locations import transform_usgs_monitoring_locations
from utils.serialization import deserialize_df


def test_transform_valid_data():
    raw_data = {
        "features": [
            {
                "id": "123",
                "properties": {
                    "dec_lat_va": "45.0",
                    "dec_long_va": "-120.0",
                    "alt_va": "500",
                    "state_cd": "01",
                    "county_cd": "001",
                    "huc_cd": "010101",
                    "agency_cd": "USGS",
                    "site_no": "00001",
                    "station_nm": "Test Station",
                    "inventory_dt": "2021-01-01"
                }
            }
        ]
    }
    blob = transform_usgs_monitoring_locations(raw_data)
    df = deserialize_df(blob)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["site_number"] == "00001"


def test_transform_empty_features():
    raw_data = {"features": []}
    blob = transform_usgs_monitoring_locations(raw_data)
    assert blob is None
