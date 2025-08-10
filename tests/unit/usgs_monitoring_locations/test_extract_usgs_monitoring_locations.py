# tests/test_extract_usgs_monitoring_locations.py

import pytest
import pandas as pd
from unittest.mock import MagicMock
from src.usgs_monitoring_locations.extract_usgs_monitoring_locations import extract_usgs_monitoring_locations
from src.utils.config import Config, DBConfig

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
