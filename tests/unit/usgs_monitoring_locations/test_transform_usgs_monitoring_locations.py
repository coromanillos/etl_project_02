# tests/test_transform_usgs_monitoring_locations.py

import pytest
import pandas as pd
from src.usgs_monitoring_locations import transform_usgs_monitoring_locations as transform_mod

def test_transform_reads_and_cleans_data(fake_config, tmp_path):
    filename = fake_config.filename_pattern.format(timestamp="20250809_123456")
    parquet_path = tmp_path / filename

    df_raw = pd.DataFrame({
        "dec_lat_va": [40.1],
        "dec_long_va": [-75.1],
        "alt_va": [100.5],
        "state_cd": ["PA"],
        "county_cd": ["001"],
        "huc_cd": ["020401"],
        "agency_cd": ["USGS"],
        "site_no": ["12345"],
        "station_nm": ["Test Station"],
        "inventory_dt": ["2025-08-09"]
    })
    df_raw.to_parquet(parquet_path)

    df_clean = transform_mod.transform_usgs_monitoring_locations(fake_config)

    expected_columns = {
        "latitude", "longitude", "elevation_ft", "state_code",
        "county_code", "huc_code", "agency_code", "site_number",
        "station_name", "inventory_date"
    }
    assert expected_columns.issubset(df_clean.columns)

    assert df_clean.loc[0, "latitude"] == 40.1
    assert df_clean.loc[0, "longitude"] == -75.1
    assert df_clean.loc[0, "elevation_ft"] == 100.5

    assert pd.api.types.is_float_dtype(df_clean["latitude"])
    assert pd.api.types.is_float_dtype(df_clean["longitude"])
    assert pd.api.types.is_float_dtype(df_clean["elevation_ft"])
    assert pd.api.types.is_datetime64_any_dtype(df_clean["inventory_date"])

def test_transform_raises_if_no_file(fake_config, tmp_path):
    fake_config.output_directory = str(tmp_path)
    with pytest.raises(FileNotFoundError):
        transform_mod.transform_usgs_monitoring_locations(fake_config)

def test_transform_raises_if_file_corrupted(fake_config, tmp_path):
    filename = fake_config.filename_pattern.format(timestamp="20250809_123456")
    bad_file = tmp_path / filename
    bad_file.write_text("this is not a parquet file")

    with pytest.raises(Exception):
        transform_mod.transform_usgs_monitoring_locations(fake_config)
