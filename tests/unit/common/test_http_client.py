# tests/unit/common/test_http_client.py

# tests/unit/test_http_client.py

import pytest
from unittest.mock import patch, Mock
from src.common.http_client import RequestsHttpClient
import requests

@pytest.fixture
def client():
    return RequestsHttpClient()

@patch("src.common.http_client.requests.get")
def test_get_success(mock_get, client):
    # Arrange: prepare mock response
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None  # no exception raised
    mock_response.status_code = 200
    mock_response.json.return_value = {"key": "value"}
    mock_get.return_value = mock_response

    # Act
    url = "http://fakeurl.test/api"
    response = client.get(url)

    # Assert
    mock_get.assert_called_once_with(url, params=None, headers=None, timeout=10)
    assert response == {"key": "value"}

@patch("src.common.http_client.requests.get")
def test_get_http_error_raises(mock_get, client):
    # Arrange: simulate an HTTP error
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("Error 404")
    mock_get.return_value = mock_response

    url = "http://fakeurl.test/api"

    # Act & Assert
    with pytest.raises(requests.HTTPError):
        client.get(url)
    mock_get.assert_called_once_with(url, params=None, headers=None, timeout=10)
