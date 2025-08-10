# tests/test_run_usgs_monitoring_locations.py

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.usgs_monitoring_locations.extract_usgs_monitoring_locations import extract_usgs_monitoring_locations
from src.utils.config import Config, DBConfig
import io
import src.usgs_monitoring_locations.run_usgs_monitoring_locations as run_mod
from src.usgs_monitoring_locations import transform_usgs_monitoring_locations as transform_mod

@pytest.fixture
def fake_config(tmp_path):
    return Config(
        monitoring_locations_url="http://fake-url",
        output_directory=str(tmp_path),
        filename_pattern="usgs_monitoring_locations_{timestamp}.parquet",
        timestamp_format="%Y%m%d_%H%M%S",
        db=DBConfig(
            user="user",
            password="pass",
            host="host",
            port=5432,
            database="db"
        )
    )

def test_extract_success(fake_config, mock_client):
    url = fake_config.monitoring_locations_url
    sample_response = {
        "features": [
            {"id": "1", "properties": {"a": 1}},
            {"id": "2", "properties": {"a": 2}}
        ]
    }
    mock_client.get.return_value = sample_response

    df = extract_usgs_monitoring_locations(url, mock_client)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "id" in df.columns
    mock_client.get.assert_called_once_with(url)

def test_extract_no_url(fake_config, mock_client):
    df = extract_usgs_monitoring_locations(None, mock_client)
    assert df is None

def test_extract_no_features(fake_config, mock_client):
    url = fake_config.monitoring_locations_url
    mock_client.get.return_value = {"features": []}

    df = extract_usgs_monitoring_locations(url, mock_client)
    assert df is None

def test_extract_raises_exception(fake_config, mock_client):
    url = fake_config.monitoring_locations_url
    mock_client.get.side_effect = Exception("fail")

    df = extract_usgs_monitoring_locations(url, mock_client)
    assert df is None

def test_extract_task_success():
    df_expected = pd.DataFrame({"a": [1]})
    http_client = MagicMock()
    with patch.object(run_mod, "extract_usgs_monitoring_locations", return_value=df_expected):
        df_result = run_mod.extract_task("http://test", http_client)
        pd.testing.assert_frame_equal(df_result, df_expected)

def test_extract_task_raises_on_empty():
    http_client = MagicMock()
    with patch.object(run_mod, "extract_usgs_monitoring_locations", return_value=pd.DataFrame()):
        with pytest.raises(ValueError, match="No data returned"):
            run_mod.extract_task("http://test", http_client)

def test_transform_task_success(fake_config):
    df_input = pd.DataFrame({"x": [1]})
    parquet_bytes = io.BytesIO()
    df_input.to_parquet(parquet_bytes)
    parquet_bytes.seek(0)

    with patch.object(run_mod, "transform_usgs_monitoring_locations", return_value=parquet_bytes.getvalue()):
        df_result = run_mod.transform_task(df_input, fake_config)
        pd.testing.assert_frame_equal(df_result, df_input)

def test_transform_task_raises_on_empty(fake_config):
    df_input = pd.DataFrame({"x": [1]})
    with patch.object(run_mod, "transform_usgs_monitoring_locations", return_value=None):
        with pytest.raises(ValueError, match="No valid data"):
            run_mod.transform_task(df_input, fake_config)

def test_load_task_calls_loader():
    df_input = pd.DataFrame({"x": [1]})
    mock_loader = MagicMock()
    with patch.object(run_mod, "load_usgs_monitoring_locations", mock_loader):
        run_mod.load_task(df_input, "factory", "meta")
        mock_loader.assert_called_once_with(df_input, "factory", "meta")

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
