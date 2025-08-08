# tests/test_extract_usgs_monitoring_locations.py

import pytest
import pandas as pd
from unittest.mock import MagicMock
from src.usgs_monitoring_locations.extract_usgs_monitoring_locations import extract_usgs_monitoring_locations

class ConfigStub:
    def __init__(self, url=None, raw_path=None):
        self.config = {
            "usgs": {
                "monitoring_locations_url": url
            },
            "data_paths": {
                "raw": raw_path or "/tmp/test_raw.parquet"
            }
        }

    def __getitem__(self, key):
        return self.config.get(key)

@pytest.fixture
def mock_client():
    return MagicMock()

def test_extract_success(tmp_path, mock_client):
    # Arrange
    url = "http://fake-url.com"
    raw_path = tmp_path / "raw.parquet"
    config = ConfigStub(url=url, raw_path=str(raw_path))

    sample_response = {
        "features": [
            {"id": "1", "properties": {"a": 1}},
            {"id": "2", "properties": {"a": 2}}
        ]
    }
    mock_client.get.return_value = sample_response

    # Act
    df = extract_usgs_monitoring_locations(config["usgs"]["monitoring_locations_url"], mock_client)

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "id" in df.columns
    mock_client.get.assert_called_once_with(url)

def test_extract_no_url(mock_client):
    config = ConfigStub(url=None)
    df = extract_usgs_monitoring_locations(config["usgs"]["monitoring_locations_url"], mock_client)
    assert df is None

def test_extract_no_features(mock_client):
    url = "http://fake-url.com"
    config = ConfigStub(url=url)
    mock_client.get.return_value = {"features": []}

    df = extract_usgs_monitoring_locations(config["usgs"]["monitoring_locations_url"], mock_client)
    assert df is None

def test_extract_raises_exception(mock_client):
    url = "http://fake-url.com"
    config = ConfigStub(url=url)
    mock_client.get.side_effect = Exception("fail")

    df = extract_usgs_monitoring_locations(config["usgs"]["monitoring_locations_url"], mock_client)
    assert df is None
