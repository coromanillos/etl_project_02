# tests/test_db_config.py

from unittest.mock import patch, MagicMock
from sqlalchemy.engine import Engine
from src.utils import db_config
from src.utils.config import Config

def test_get_engine(monkeypatch):
    # Create a Config instance with necessary DB config fields
    # Add DB config fields to Config or mock them as attributes here
    mock_config = Config(
        monitoring_locations_url="https://mock-url.com",
        output_directory="mock_dir",
        filename_pattern="file_{timestamp}.json",
        timestamp_format="%Y%m%d_%H%M%S",
        api_key="secret",
    )

    # Add DB config as attributes for compatibility
    # If you have a dedicated DB config, adapt accordingly
    setattr(mock_config, "db_user", "test_user")
    setattr(mock_config, "db_password", "test_pass")
    setattr(mock_config, "db_host", "localhost")
    setattr(mock_config, "db_port", 5432)
    setattr(mock_config, "db_name", "test_db")

    monkeypatch.setattr("src.utils.db_config.load_config", lambda path=None: mock_config)

    with patch("src.utils.db_config.create_engine") as mock_create_engine:
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        engine = db_config.get_engine()
        expected_url = "postgresql://test_user:test_pass@localhost:5432/test_db"
        mock_create_engine.assert_called_once_with(expected_url)
        assert engine == mock_engine
