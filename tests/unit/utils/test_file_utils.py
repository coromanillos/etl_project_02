# tests/unit/utils/test_file_utils.py

import json
import pandas as pd
import pytest
from src.exceptions import SaveError, TransformError


def test_save_file_and_load(data_manager):
    records = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    path = data_manager.save_file(records, endpoint="stations")
    assert path.exists()

    loaded = data_manager.load_latest_file("stations")
    assert loaded == records


def test_save_file_empty_list_logs_warning(data_manager, mock_logger):
    path = data_manager.save_file([], endpoint="stations")
    assert path is None
    mock_logger.warning.assert_called_once()


def test_save_file_raises_on_error(data_manager, monkeypatch):
    monkeypatch.setattr("builtins.open", lambda *a, **k: (_ for _ in ()).throw(OSError("disk error")))
    with pytest.raises(SaveError):
        data_manager.save_file([{"id": 1}], "stations")


def test_save_dataframe_csv(data_manager):
    df = pd.DataFrame([{"id": 1, "value": "x"}])
    path = data_manager.save_dataframe(df, "stations", output_format="csv")
    assert path.suffix == ".csv"
    loaded = pd.read_csv(path)
    assert loaded.equals(df)


def test_save_dataframe_parquet(data_manager):
    df = pd.DataFrame([{"id": 1, "value": "x"}])
    path = data_manager.save_dataframe(df, "stations", output_format="parquet")
    assert path.suffix == ".parquet"
    loaded = pd.read_parquet(path)
    assert loaded.equals(df)


def test_save_dataframe_invalid_format(data_manager):
    df = pd.DataFrame([{"id": 1}])
    with pytest.raises(ValueError):
        data_manager.save_dataframe(df, "stations", output_format="xml")


def test_load_latest_file_no_files(data_manager):
    with pytest.raises(FileNotFoundError):
        data_manager.load_latest_file("no_such_endpoint")


def test_load_latest_file_corrupt_json(data_manager):
    # write bad JSON
    bad_file = data_manager._get_directory("stations") / "stations_123.json"
    bad_file.write_text("{not: valid}")
    with pytest.raises(TransformError):
        data_manager.load_latest_file("stations")
