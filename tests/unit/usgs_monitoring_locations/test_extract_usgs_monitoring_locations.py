# test_extract_usgs_monitoring_locations.py

import pytest
from unittest.mock import patch
from src.extract_usgs_monitoring_locations import fetch_json, extract_usgs_monitoring_locations


@patch("src.extract_usgs_monitoring_locations.requests.get")
def test_fetch_json_success(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"data": "ok"}

    result = fetch_json("http://example.com")
    assert result == {"data": "ok"}, "Should return parsed JSON data"


@patch("src.extract_usgs_monitoring_locations.requests.get")
def test_fetch_json_failure(mock_get):
    mock_get.side_effect = Exception("Connection error")

    result = fetch_json("http://bad-url.com")
    assert result is None, "Should return None on request failure"


def test_extract_usgs_monitoring_locations_missing_config():
    result = extract_usgs_monitoring_locations({})
    assert result is None, "Should return None when config is missing"


@patch("src.extract_usgs_monitoring_locations.fetch_json")
def test_extract_usgs_monitoring_locations_valid(mock_fetch, mock_config):
    mock_fetch.return_value = {"mock": "data"}

    result = extract_usgs_monitoring_locations(mock_config)
    assert result == {"mock": "data"}, "Should return data fetched via config URL"
