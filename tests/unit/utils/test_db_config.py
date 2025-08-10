# tests/test_db_config.py

import pytest
from unittest.mock import patch, MagicMock
from src.utils import db_config

def test_get_engine_builds_correct_url(fake_config):
    mock_engine_instance = MagicMock()
    with patch("src.utils.db_config.create_engine", return_value=mock_engine_instance) as mock_create_engine:
        result = db_config.get_engine(fake_config)
        expected_url = (
            f"postgresql://{fake_config.db.user}:{fake_config.db.password}"
            f"@{fake_config.db.host}:{fake_config.db.port}/{fake_config.db.database}"
        )
        mock_create_engine.assert_called_once_with(expected_url)
        assert result is mock_engine_instance

def test_get_engine_loads_default_config():
    mock_config = fake_config  # or create a local Config object here if you want
    mock_engine_instance = MagicMock()

    with patch("src.utils.db_config.load_config", return_value=mock_config) as mock_load_config, \
         patch("src.utils.db_config.create_engine", return_value=mock_engine_instance) as mock_create_engine:
        result = db_config.get_engine()
        expected_url = f"postgresql://{mock_config.db.user}:{mock_config.db.password}@{mock_config.db.host}:{mock_config.db.port}/{mock_config.db.database}"
        mock_load_config.assert_called_once_with("config/config.yaml")
        mock_create_engine.assert_called_once_with(expected_url)
        assert result is mock_engine_instance
