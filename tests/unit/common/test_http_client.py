# tests/unit/common/test_http_client.py

import pytest
from unittest.mock import patch, Mock
from src.common.http_client import RequestsHttpClient


@patch("src.common.http_client.requests.get")
def test_get_success(mock_get):
    # Arrange
    url = "https://example.com/data"
    expected_json = {"key": "value"}

    mock_response = Mock()
    mock_response.json.return_value = expected_json
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    client = RequestsHttpClient()

    # Act
    result = client.get(url)

    # Assert
    mock_get.assert_called_once_with(url, params=None, headers=None, timeout=10)
    assert result == expected_json


@patch("src.common.http_client.requests.get")
def test_get_raises_exception(mock_get):
    # Arrange
    url = "https://example.com/data"
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Exception("HTTP error")
    mock_get.return_value = mock_response

    client = RequestsHttpClient()

    # Act & Assert
    with pytest.raises(Exception, match="HTTP error"):
        client.get(url)
