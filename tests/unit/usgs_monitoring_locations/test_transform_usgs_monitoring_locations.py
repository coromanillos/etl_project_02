# tests/test_transform_usgs_monitoring_locations.py

import pytest
import pandas as pd
from unittest.mock import patch
from src.usgs_monitoring_locations.transform_usgs_monitoring_locations import transform_usgs_monitoring_locations
from src.utils.config import Config

@pytest.fixture
def sample_parquet(tmp_path):
    df = pd.DataFrame({
        "dec_lat_va": [10.0, None],
        "dec_long_va": [20.0, 30.0],
        "alt_va": [100, None],
        "state_cd": ["CA", "TX"],
        "county_cd": ["001", "002"],
        "huc_cd": ["01", "02"],
        "agency_cd": ["A", "B"],
        "site_no": ["001", "002"],
        "station_nm": ["Station1", "Station2"],
        "inventory_dt": ["2020-01-01", None]
    })
    path = tmp_path / "raw.parquet"
    df.to_parquet(path)
    return path

def test_transform_success(monkeypatch, sample_parquet):
    # Patch load_config to return a Config instance with output directory & filename_pattern
    mock_config = Config(
        monitoring_locations_url="https://mock-url.com",
        output_directory=str(sample_parquet.parent),
        filename_pattern=sample_parquet.name,
        timestamp_format="%Y%m%d_%H%M%S"
    )
    monkeypatch.setattr("src.usgs_monitoring_locations.transform_usgs_monitoring_locations.load_config", lambda path=None: mock_config)

    result_df = transform_usgs_monitoring_locations()
    assert "latitude" in result_df.columns
    assert len(result_df) == 2
    assert result_df["latitude"].iloc[0] == 10.0

def test_transform_failure(monkeypatch):
    mock_config = Config(
        monitoring_locations_url="https://mock-url.com",
        output_directory="/non/existent",
        filename_pattern="missing.parquet",
        timestamp_format="%Y%m%d_%H%M%S"
    )
    monkeypatch.setattr("src.usgs_monitoring_locations.transform_usgs_monitoring_locations.load_config", lambda path=None: mock_config)

    with pytest.raises(Exception):
        transform_usgs_monitoring_locations()
