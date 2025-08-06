# tests/test_db_config.py

from unittest.mock import patch, MagicMock
from sqlalchemy.engine import Engine
from src.utils import db_config

def test_get_engine_connection_string(monkeypatch):
    # Patch config values
    monkeypatch.setattr(db_config.config, "postgres_user", "test_user")
    monkeypatch.setattr(db_config.config, "postgres_password", "test_pass")
    monkeypatch.setattr(db_config.config, "postgres_host", "localhost")
    monkeypatch.setattr(db_config.config, "postgres_port", 5432)
    monkeypatch.setattr(db_config.config, "postgres_db", "test_db")

    with patch("src.utils.db_config.create_engine") as mock_create_engine:
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        engine = db_config.get_engine()

        expected_url = "postgresql://test_user:test_pass@localhost:5432/test_db"
        mock_create_engine.assert_called_once_with(expected_url)
        assert engine == mock_engine
