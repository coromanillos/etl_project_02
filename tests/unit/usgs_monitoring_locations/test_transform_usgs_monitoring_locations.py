# tests/test_transform_usgs_monitoring_locations.py

import pytest
import pandas as pd
from unittest.mock import patch
from src.usgs_monitoring_locations.transform_usgs_monitoring_locations import transform_usgs_monitoring_locations
from src.utils.config import Config, DBConfig  # assuming DBConfig is nested inside config.py


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


def make_mock_config(**overrides) -> Config:
    """
    Helper to create a minimal valid Config with defaults that work for tests.
    Allows overriding any field via kwargs.
    """
    defaults = dict(
        monitoring_locations_url="https://mock-url.com",
        output_directory="/tmp",  # will be overridden per test
        filename_pattern="raw.parquet",
        timestamp_format="%Y%m%d_%H%M%S",
        db=DBConfig(  # supply nested DBConfig so .db.user still works
            user="test_user",
            password="test_pass",
            host="localhost",
            port="5432",
            database="test_db"
        )
    )
    defaults.update(overrides)
    return Config(**defaults)


def test_transform_success(monkeypatch, sample_parquet):
    mock_config = make_mock_config(
        output_directory=str(sample_parquet.parent),
        filename_pattern=sample_parquet.name
    )
    monkeypatch.setattr(
        "src.usgs_monitoring_locations.transform_usgs_monitoring_locations.load_config",
        lambda path=None: mock_config
    )

    result_df = transform_usgs_monitoring_locations()
    assert "latitude" in result_df.columns
    assert len(result_df) == 2
    assert result_df["latitude"].iloc[0] == 10.0


def test_transform_failure(monkeypatch):
    mock_config = make_mock_config(
        output_directory="/non/existent",
        filename_pattern="missing.parquet"
    )
    monkeypatch.setattr(
        "src.usgs_monitoring_locations.transform_usgs_monitoring_locations.load_config",
        lambda path=None: mock_config
    )

    with pytest.raises(Exception):
        transform_usgs_monitoring_locations()
