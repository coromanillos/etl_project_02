# tests/unit/test_usgs_loader.py

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.usgs_loader import USGSLoader
from src.exceptions import LoaderError


@pytest.fixture
def mock_config():
    return {
        "db_config": {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "user",
            "password": "pass",
        },
        "endpoint_config": {
            "load_mode": "upsert",
            "primary_key": ["id"],
        },
    }


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_data_manager():
    return MagicMock()


def make_loader(mock_config, mock_logger, mock_data_manager):
    return USGSLoader(
        data_manager=mock_data_manager,
        logger=mock_logger,
        db_config=mock_config["db_config"],
        endpoint_config=mock_config["endpoint_config"],
    )


def test_init_missing_keys_raises():
    with pytest.raises(ValueError):
        USGSLoader(logger=MagicMock())  # missing keys


@patch("src.usgs_loader.psycopg2.connect")
def test_connect_success(mock_connect, mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    loader.connect()
    assert loader.conn == mock_conn
    mock_logger.info.assert_called_with("Connected to PostGIS")


@patch("src.usgs_loader.psycopg2.connect", side_effect=Exception("fail"))
def test_connect_failure_raises(mock_connect, mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    with pytest.raises(LoaderError):
        loader.connect()


def test_close_closes_connection(mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    loader.conn = MagicMock()
    loader.close()
    loader.conn.close.assert_called_once()
    mock_logger.info.assert_called_with("Closed PostGIS connection")


def test_load_dataframe_empty_df_logs_warning(mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    loader.conn = MagicMock()

    df = pd.DataFrame()
    loader.load_dataframe(df, "public.test")
    mock_logger.warning.assert_called_with("No records to load into public.test")


def test_load_dataframe_replace_executes_truncate_and_insert(mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    loader.conn = MagicMock()
    cur = loader.conn.cursor.return_value.__enter__.return_value

    df = pd.DataFrame([{"id": 1, "val": "a"}])
    loader.load_dataframe(df, "public.test", mode="replace")

    cur.execute.assert_any_call('TRUNCATE TABLE public.test RESTART IDENTITY CASCADE')
    loader.conn.commit.assert_called_once()
    mock_logger.info.assert_any_call("Truncated public.test before reloading")


def test_load_dataframe_upsert_executes_insert(mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    loader.conn = MagicMock()
    cur = loader.conn.cursor.return_value.__enter__.return_value

    df = pd.DataFrame([{"id": 1, "val": "a"}])
    loader.load_dataframe(df, "public.test", mode="upsert")

    assert cur.execute.call_count == 0  # only execute_values used
    loader.conn.commit.assert_called_once()


def test_load_dataframe_invalid_mode_raises(mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    loader.conn = MagicMock()
    df = pd.DataFrame([{"id": 1}])

    with pytest.raises(LoaderError):
        loader.load_dataframe(df, "public.test", mode="invalid")


def test_load_dataframe_rollback_on_failure(mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    loader.conn = MagicMock()
    cur = loader.conn.cursor.return_value.__enter__.return_value
    cur.execute.side_effect = Exception("db fail")

    df = pd.DataFrame([{"id": 1}])
    with pytest.raises(LoaderError):
        loader.load_dataframe(df, "public.test")

    loader.conn.rollback.assert_called_once()


def test_load_latest_file_success(monkeypatch, mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)

    df = pd.DataFrame([{"id": 1, "val": "a"}])
    mock_data_manager.load_latest_file.return_value = df
    loader.connect = MagicMock()
    loader.load_dataframe = MagicMock()
    loader.close = MagicMock()

    result = loader.load_latest_file("endpoint")

    assert "Successfully upsert-loaded" in result
    loader.connect.assert_called_once()
    loader.load_dataframe.assert_called_once()
    loader.close.assert_called_once()


def test_load_latest_file_failure(mock_config, mock_logger, mock_data_manager):
    loader = make_loader(mock_config, mock_logger, mock_data_manager)
    mock_data_manager.load_latest_file.side_effect = Exception("file error")

    loader.connect = MagicMock()
    loader.close = MagicMock()

    with pytest.raises(LoaderError):
        loader.load_latest_file("endpoint")

    mock_logger.error.assert_called()
    loader.close.assert_called_once()
