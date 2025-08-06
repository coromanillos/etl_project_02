# tests/test_run_usgs_monitoring_locations.py
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.usgs_monitoring_locations.run_usgs_monitoring_locations import extract_task, transform_task, load_task

def test_extract_task_success():
    mock_config = MagicMock()
    mock_client = MagicMock()

    sample_df = pd.DataFrame({"a": [1,2]})
    with patch("src.usgs_monitoring_locations.extract_usgs_monitoring_locations.extract_usgs_monitoring_locations", return_value=sample_df):
        df = extract_task(mock_config, mock_client)
    assert not df.empty

def test_extract_task_failure():
    mock_config = MagicMock()
    mock_client = MagicMock()

    with patch("src.usgs_monitoring_locations.extract_usgs_monitoring_locations.extract_usgs_monitoring_locations", return_value=None):
        with pytest.raises(ValueError):
            extract_task(mock_config, mock_client)

def test_transform_task_success():
    sample_df = pd.DataFrame({"a": [1,2]})
    with patch("src.usgs_monitoring_locations.transform_usgs_monitoring_locations.transform_usgs_monitoring_locations", return_value=sample_df.to_parquet()):
        transformed_df = transform_task(sample_df)
    assert not transformed_df.empty

def test_transform_task_failure():
    sample_df = pd.DataFrame({"a": [1,2]})
    with patch("src.usgs_monitoring_locations.transform_usgs_monitoring_locations.transform_usgs_monitoring_locations", return_value=None):
        with pytest.raises(ValueError):
            transform_task(sample_df)

def test_load_task_calls_load(monkeypatch):
    df = pd.DataFrame({"a": [1,2]})
    called = {}
    def fake_load(df_arg, session_factory, metadata):
        called["called"] = True
        assert isinstance(df_arg, pd.DataFrame)

    monkeypatch.setattr("src.usgs_monitoring_locations.load_usgs_monitoring_locations.load_usgs_monitoring_locations", fake_load)
    load_task(df, session_factory=None, metadata=None)
    assert called.get("called", False)
