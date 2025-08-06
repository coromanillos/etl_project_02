# tests/test_transform_usgs_monitoring_locations.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.usgs_monitoring_locations.transform_usgs_monitoring_locations import transform_usgs_monitoring_locations

@pytest.fixture
def sample_raw_df(tmp_path):
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
    # Save to temp parquet
    path = tmp_path / "raw.parquet"
    df.to_parquet(path)
    return path

def test_transform_success(monkeypatch, tmp_path):
    # Patch config singleton to use tmp_path
    monkeypatch.setattr("src.usgs_monitoring_locations.transform_usgs_monitoring_locations.config", 
                        MagicMock(data_paths={"raw": tmp_path / "raw.parquet", "prepared": tmp_path / "prepared.parquet"}))

    # Write sample parquet file for input
    df_in = pd.DataFrame({
        "dec_lat_va": [10.0],
        "dec_long_va": [20.0],
        "alt_va": [100],
        "state_cd": ["CA"],
        "county_cd": ["001"],
        "huc_cd": ["01"],
        "agency_cd": ["A"],
        "site_no": ["001"],
        "station_nm": ["Station1"],
        "inventory_dt": ["2020-01-01"]
    })
    df_in.to_parquet(tmp_path / "raw.parquet")

    # Run transform
    result_df = transform_usgs_monitoring_locations()

    assert "latitude" in result_df.columns
    assert len(result_df) == 1
    assert result_df["latitude"].iloc[0] == 10.0

def test_transform_failure(monkeypatch):
    monkeypatch.setattr("src.usgs_monitoring_locations.transform_usgs_monitoring_locations.config", 
                        MagicMock(data_paths={"raw": "/non/existent/path.parquet", "prepared": "/tmp/prep.parquet"}))
    with pytest.raises(Exception):
        transform_usgs_monitoring_locations()
