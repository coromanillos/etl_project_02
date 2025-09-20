# tests/unit/test_usgs_archiver.py

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from src.usgs_archiver import USGSArchiver
from src.exceptions import ArchiverError


@pytest.fixture
def mock_dependencies(tmp_path):
    """Fixture providing mocked dependencies and a temporary archive dir."""
    mock_dm = MagicMock()
    mock_logger = MagicMock()
    config = {"paths": {"archived_data": str(tmp_path / "archive")}}
    return mock_dm, mock_logger, config


def test_archive_success(mock_dependencies, tmp_path):
    mock_dm, mock_logger, config = mock_dependencies
    # Create dummy source file
    source_file = tmp_path / "test.json"
    source_file.write_text('{"key": "value"}')
    mock_dm.get_latest_file.return_value = source_file

    archiver = USGSArchiver(data_manager=mock_dm, logger=mock_logger, config=config)
    archived_path = archiver.archive_latest_file("test_endpoint")

    # Validate return and file contents
    assert archived_path is not None
    archived_file = Path(archived_path)
    assert archived_file.exists()
    assert archived_file.read_text() == '{"key": "value"}'

    # Validate logger calls
    mock_logger.info.assert_called_once()
    assert "Archived" in mock_logger.info.call_args[0][0]


def test_archive_no_file(mock_dependencies):
    mock_dm, mock_logger, config = mock_dependencies
    mock_dm.get_latest_file.return_value = None

    archiver = USGSArchiver(data_manager=mock_dm, logger=mock_logger, config=config)
    result = archiver.archive_latest_file("test_endpoint")

    assert result is None
    mock_logger.warning.assert_called_once()
    assert "No file to archive" in mock_logger.warning.call_args[0][0]


def test_missing_required_kwargs_raises():
    with pytest.raises(ValueError) as exc:
        USGSArchiver(config={})
    assert "Missing required arguments" in str(exc.value)


def test_invalid_config_raises(mock_dependencies):
    mock_dm, mock_logger, _ = mock_dependencies
    bad_config = {"paths": {}}  # missing archived_data
    with pytest.raises(ArchiverError):
        USGSArchiver(data_manager=mock_dm, logger=mock_logger, config=bad_config)


def test_copy_failure_raises_archiver_error(mock_dependencies, tmp_path):
    mock_dm, mock_logger, config = mock_dependencies
    # Point to a file that doesn’t exist
    missing_file = tmp_path / "does_not_exist.json"
    mock_dm.get_latest_file.return_value = missing_file

    archiver = USGSArchiver(data_manager=mock_dm, logger=mock_logger, config=config)

    with pytest.raises(ArchiverError) as exc:
        archiver.archive_latest_file("test_endpoint")

    assert "Archiving failed" in str(exc.value)
    mock_logger.error.assert_called_once()


def test_get_timestamped_path_creates_dir(mock_dependencies):
    mock_dm, mock_logger, config = mock_dependencies
    archiver = USGSArchiver(data_manager=mock_dm, logger=mock_logger, config=config)

    endpoint = "stations"
    path = archiver._get_timestamped_path(endpoint)

    assert path.exists()
    assert endpoint in str(path)
    assert path.is_dir()


def test_copy_file_copies_content(mock_dependencies, tmp_path):
    mock_dm, mock_logger, config = mock_dependencies
    archiver = USGSArchiver(data_manager=mock_dm, logger=mock_logger, config=config)

    # Create a dummy file
    src = tmp_path / "source.json"
    src.write_text('{"hello": "world"}')

    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()

    dest_file = archiver._copy_file(src, dest_dir)

    assert dest_file.exists()
    assert dest_file.read_text() == '{"hello": "world"}'
