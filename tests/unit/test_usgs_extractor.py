# tests/unit/test_usgs_extractor.py

import pytest
from unittest.mock import MagicMock

from src.usgs_extractor import USGSExtractor
from src.exceptions import ExtractionError


def test_missing_required_keys_raises(base_extractor_config, mock_logger, mock_data_manager):
    """Extractor should raise if critical kwargs are missing."""
    with pytest.raises(ValueError) as excinfo:
        USGSExtractor(config=base_extractor_config, endpoint_key="stations")
    assert "Missing required arguments" in str(excinfo.value)


def test_build_url_adds_limit_and_offset(extractor, base_extractor_config):
    """build_url should correctly add query params."""
    url = extractor.build_url(offset=10, limit=5, custom="yes")
    assert "offset=10" in url
    assert "limit=5" in url
    assert "custom=yes" in url
    assert url.startswith(base_extractor_config["usgs"]["stations"]["base_url"])


def test_fetch_with_retries_success(extractor):
    """fetch_with_retries should return JSON if request succeeds."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"features": [1, 2, 3]}
    mock_response.raise_for_status.return_value = None
    extractor.http_client.get.return_value = mock_response

    result = extractor.fetch_with_retries("http://fake-url")
    assert result == {"features": [1, 2, 3]}
    extractor.http_client.get.assert_called_once()


def test_fetch_with_retries_exhausts_retries(extractor):
    """Should raise ExtractionError after max retries."""
    extractor.http_client.get.side_effect = Exception("fail")

    with pytest.raises(ExtractionError) as excinfo:
        extractor.fetch_with_retries("http://fake-url")

    assert "Failed to fetch data" in str(excinfo.value)
    assert extractor.logger.warning.call_count == extractor.endpoint_config["max_retries"]


def test_fetch_all_records_full_mode(extractor, mock_data_manager):
    """fetch_all_records should paginate until no features remain."""
    extractor.fetch_with_retries = MagicMock(
        side_effect=[
            {"features": [{"id": 1}, {"id": 2}]},
            {"features": []},
        ]
    )

    records = extractor.fetch_all_records()
    assert len(records) == 2
    extractor.logger.info.assert_any_call("No more data returned; extraction complete")
    mock_data_manager.save_file.assert_called_once()


def test_fetch_recent_records(extractor, mock_data_manager):
    """fetch_recent should fetch and save limited records."""
    extractor.endpoint_config["mode"] = "recent"
    extractor.fetch_with_retries = MagicMock(
        return_value={"features": [{"id": "recent"}]}
    )

    records = extractor.fetch_recent()
    assert records == [{"id": "recent"}]
    extractor.logger.info.assert_any_call("Fetched 1 recent records")
    mock_data_manager.save_file.assert_called_once()
