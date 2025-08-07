# tests/test_db_config.py

from unittest.mock import patch, MagicMock
from sqlalchemy.engine import Engine
from src.utils import db_config

def test_get_engine(monkeypatch):
    mock_config = {
        "db": {
            "user": "test_user",
            "password": "test_pass",
            "host": "localhost",
            "port": 5432,
            "name": "test_db"
        }
    }
    monkeypatch.setattr("src.utils.db_config.load_config", lambda path=None: mock_config)

    with patch("src.utils.db_config.create_engine") as mock_create_engine:
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        engine = db_config.get_engine()
        expected_url = "postgresql://test_user:test_pass@localhost:5432/test_db"
        mock_create_engine.assert_called_once_with(expected_url)
        assert engine == mock_engine
