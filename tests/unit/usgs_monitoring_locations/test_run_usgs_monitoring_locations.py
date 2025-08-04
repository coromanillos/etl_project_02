# test_usgs_run_monitoring_locations.py

import pytest
import pandas as pd
from unittest.mock import patch
from src.run_usgs_monitoring_locations import extract_task, transform_task, load_task


@pytest.fixture
def sample_raw_data():
    return {"features": [{"properties": {"site_no": "001", "station_nm": "Test Site"}}]}


@pytest.fixture
def sample_transformed_data():
    return [{"site_no": "001", "station_nm": "Test Site"}]


def test_extract_task_success():
    with patch("src.run_usgs_monitoring_locations.load_config", return_value={"mock": "config"}), \
         patch("src.run_usgs_monitoring_locations.extract_usgs_monitoring_locations", return_value={"mock": "data"}):
        result = extract_task()
        assert isinstance(result, dict)
        assert result == {"mock": "data"}


def test_extract_task_failure():
    with patch("src.run_usgs_monitoring_locations.load_config", return_value={"mock": "config"}), \
         patch("src.run_usgs_monitoring_locations.extract_usgs_monitoring_locations", return_value=None):
        with pytest.raises(ValueError, match="Extract task failed: No data returned."):
            extract_task()


def test_transform_task_success(sample_raw_data):
    with patch("src.run_usgs_monitoring_locations.parse_usgs_monitoring_locations") as mock_parse:
        mock_df = pd.DataFrame(sample_raw_data["features"])
        mock_parse.return_value = mock_df
        result = transform_task(sample_raw_data)
        assert isinstance(result, list)
        assert result[0]["properties"]["site_no"] == "001"


def test_transform_task_failure_empty():
    with patch("src.run_usgs_monitoring_locations.parse_usgs_monitoring_locations", return_value=pd.DataFrame()):
        with pytest.raises(ValueError, match="Transform task failed: No valid data."):
            transform_task({})


def test_load_task_success(sample_transformed_data):
    with patch("src.run_usgs_monitoring_locations.load_data_to_postgres") as mock_load:
        load_task(sample_transformed_data)
        mock_load.assert_called_once()
