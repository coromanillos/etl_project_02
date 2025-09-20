# tests/conftest.py

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.usgs_archiver import USGSArchiver
from src.usgs_exporter import USGSExporter
from src.usgs_extractor import USGSExtractor
from src.usgs_loader import USGSLoader
from src.usgs_transformer import USGSTransformer

# ------------------------
# Common Fixtures
# ------------------------

@pytest.fixture
def mock_logger():
    """Reusable mock logger for all tests."""
    return MagicMock()

@pytest.fixture
def tmp_archive_dir(tmp_path):
    """Temporary directory for archiver tests."""
    return tmp_path / "archive"

@pytest.fixture
def mock_data_manager():
    """Reusable mock data manager."""
    return MagicMock()

# ------------------------
# USGS Archiver Fixtures
# ------------------------

@pytest.fixture
def mock_archiver_dependencies(tmp_archive_dir):
    """Provides mock data_manager, logger, and config for USGSArchiver."""
    mock_dm = MagicMock()
    mock_logger = MagicMock()
    config = {"paths": {"archived_data": str(tmp_archive_dir)}}
    return mock_dm, mock_logger, config

@pytest.fixture
def archiver(mock_archiver_dependencies):
    """Returns a pre-initialized USGSArchiver."""
    mock_dm, mock_logger, config = mock_archiver_dependencies
    return USGSArchiver(data_manager=mock_dm, logger=mock_logger, config=config)

# ------------------------
# USGS Exporter Fixtures
# ------------------------

@pytest.fixture
def base_export_config(tmp_path):
    """Minimal working config with paths and exports defined."""
    return {
        "db": {
            "user": "test_user",
            "password": "test_pass",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
        },
        "paths": {"exports": str(tmp_path)},
        "exports": {
            "gis_export": {"view_name": "test_view", "format": "gpkg"},
            "biz_export": {"view_name": "test_view", "format": "csv"},
        },
    }

@pytest.fixture
def exporter(base_export_config, mock_logger):
    """Returns a USGSExporter with a mocked engine."""
    with patch("src.usgs_exporter.create_engine") as mock_engine:
        mock_engine.return_value = MagicMock()
        return USGSExporter(config=base_export_config, logger=mock_logger)

# ------------------------
# USGS Extractor Fixtures
# ------------------------

@pytest.fixture
def base_extractor_config():
    return {
        "usgs": {
            "stations": {
                "base_url": "https://example.com/api?",
                "max_retries": 2,
                "request_timeout": 1,
                "limit": 2,
                "mode": "full",
                "extra_params": {"format": "json"},
            }
        }
    }

@pytest.fixture
def extractor(base_extractor_config, mock_logger, mock_data_manager):
    """Returns a USGSExtractor instance with mocked HTTP client."""
    return USGSExtractor(
        config=base_extractor_config,
        endpoint_key="stations",
        logger=mock_logger,
        data_manager=mock_data_manager,
        http_client=MagicMock(),
    )

# ------------------------
# USGS Loader Fixtures
# ------------------------

@pytest.fixture
def mock_loader_config():
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
def loader_factory(mock_loader_config, mock_logger, mock_data_manager):
    """Factory to create USGSLoader instances."""
    def _make_loader():
        return USGSLoader(
            data_manager=mock_data_manager,
            logger=mock_logger,
            db_config=mock_loader_config["db_config"],
            endpoint_config=mock_loader_config["endpoint_config"],
        )
    return _make_loader

# ------------------------
# USGS Transformer Fixtures
# ------------------------

@pytest.fixture
def base_transformer_config():
    return {
        "transformations": {
            "active": "yesno_to_bool",
            "created_at": "parse_datetime",
            "geometry": "flatten_geometry",
        }
    }

@pytest.fixture
def transformer(base_transformer_config, mock_logger, mock_data_manager):
    """Returns a USGSTransformer with base config."""
    return USGSTransformer(
        data_manager=mock_data_manager,
        logger=mock_logger,
        endpoint_config=base_transformer_config,
    )
