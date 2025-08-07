# tests/test_transform_usgs_monitoring_locations.py

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.usgs_monitoring_locations.transform_usgs_monitoring_locations import transform_usgs_monitoring_locations

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
    # Patch load_config to return a dict with raw path
    monkeypatch.setattr("src.usgs_monitoring_locations.transform_usgs_monitoring_locations.load_config", 
                        lambda path=None: {
                            "output": {
                                "directory": str(sample_parquet.parent),
                                "filename_pattern": sample_parquet.name
                            }
                        })

    result_df = transform_usgs_monitoring_locations()
    assert "latitude" in result_df.columns
    assert len(result_df) == 2
    assert result_df["latitude"].iloc[0] == 10.0

def test_transform_failure(monkeypatch):
    monkeypatch.setattr("src.usgs_monitoring_locations.transform_usgs_monitoring_locations.load_config", 
                        lambda path=None: {
                            "output": {
                                "directory": "/non/existent",
                                "filename_pattern": "missing.parquet"
                            }
                        })
    with pytest.raises(Exception):
        transform_usgs_monitoring_locations()
