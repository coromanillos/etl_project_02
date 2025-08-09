# test_config.py
import pytest
from unittest.mock import mock_open, patch
from pydantic import ValidationError
from src.utils.config import load_config, load_yaml_config, deep_merge

@pytest.fixture(autouse=True)
def clear_env_vars(monkeypatch):
    # Clear env vars before each test to avoid side effects
    for var in [
        "API_URL", "API_KEY", "OUTPUT_DIR",
        "FILENAME_PATTERN", "TIMESTAMP_FORMAT"
    ]:
        monkeypatch.delenv(var, raising=False)
    yield


def test_deep_merge_basic():
    d1 = {"a": 1, "b": {"c": 2}}
    d2 = {"b": {"d": 3}, "e": 4}
    merged = deep_merge(d1, d2)
    assert merged == {"a": 1, "b": {"c": 2, "d": 3}, "e": 4}
    assert d1 == {"a": 1, "b": {"c": 2}}
    assert d2 == {"b": {"d": 3}, "e": 4}


def test_load_yaml_config_success():
    yaml_content = """
usgs:
  monitoring_locations_url: "https://yaml-url.com"
output:
  directory: "yaml_dir"
  filename_pattern: "file_{timestamp}.json"
  timestamp_format: "%Y-%m-%d"
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        config = load_yaml_config("dummy_path.yaml")
    assert config["usgs"]["monitoring_locations_url"] == "https://yaml-url.com"
    assert config["output"]["directory"] == "yaml_dir"


def test_load_yaml_config_file_not_found():
    config = load_yaml_config("nonexistent.yaml")
    assert config == {}


def test_load_yaml_config_invalid_yaml():
    bad_yaml = "bad: [unbalanced brackets"
    m = mock_open(read_data=bad_yaml)
    with patch("builtins.open", m):
        config = load_yaml_config("dummy.yaml")
    assert config == {}


def test_load_config_env_overrides(monkeypatch):
    monkeypatch.setenv("API_URL", "https://env-url.com")
    monkeypatch.setenv("API_KEY", "secret-key")
    monkeypatch.setenv("OUTPUT_DIR", "env_dir")
    monkeypatch.setenv("FILENAME_PATTERN", "env_file_{timestamp}.json")
    monkeypatch.setenv("TIMESTAMP_FORMAT", "%H%M%S")

    yaml_content = """
usgs:
  monitoring_locations_url: "https://yaml-url.com"
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        config = load_config("dummy.yaml")

    # Access nested attributes
    assert config.usgs.monitoring_locations_url == "https://env-url.com"
    assert config.api_key == "secret-key"
    assert config.output.directory == "env_dir"
    assert config.output.filename_pattern == "env_file_{timestamp}.json"
    assert config.output.timestamp_format == "%H%M%S"


def test_load_config_yaml_only():
    yaml_content = """
usgs:
  monitoring_locations_url: "https://yaml-url.com"
output:
  directory: "yaml_dir"
  filename_pattern: "yaml_file_{timestamp}.json"
  timestamp_format: "%Y-%m-%d"
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        config = load_config("dummy.yaml")

    assert config.usgs.monitoring_locations_url == "https://yaml-url.com"
    assert config.api_key is None
    assert config.output.directory == "yaml_dir"
    assert config.output.filename_pattern == "yaml_file_{timestamp}.json"
    assert config.output.timestamp_format == "%Y-%m-%d"


def test_load_config_defaults():
    config = load_config(None)
    assert config.usgs.monitoring_locations_url == "https://mock-url.com"
    assert config.api_key is None
    assert config.output.directory == "mock_data"
    assert config.output.filename_pattern == "usgs_monitoring_locations_{timestamp}.json"
    assert config.output.timestamp_format == "%Y%m%d_%H%M%S"


def test_config_validation_error():
    yaml_content = """
usgs:
  monitoring_locations_url: 123
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        with pytest.raises(ValidationError):
            load_config("dummy.yaml")
