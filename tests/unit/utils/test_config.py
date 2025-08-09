# tests/test_config.py

import os
import pytest
import yaml
from unittest.mock import mock_open, patch
from pydantic import ValidationError
from src.utils.config import load_config, load_yaml_config, deep_merge, Config

@pytest.fixture(autouse=True)
def clear_env_vars(monkeypatch):
    # Clear env vars before each test to avoid side effects
    monkeypatch.delenv("API_URL", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("OUTPUT_DIR", raising=False)
    monkeypatch.delenv("FILENAME_PATTERN", raising=False)
    monkeypatch.delenv("TIMESTAMP_FORMAT", raising=False)
    yield

def test_deep_merge_basic():
    d1 = {"a": 1, "b": {"c": 2}}
    d2 = {"b": {"d": 3}, "e": 4}
    merged = deep_merge(d1, d2)
    assert merged == {"a": 1, "b": {"c": 2, "d": 3}, "e": 4}
    # Original dicts unchanged
    assert d1 == {"a": 1, "b": {"c": 2}}
    assert d2 == {"b": {"d": 3}, "e": 4}

def test_load_yaml_config_success(monkeypatch):
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

def test_load_yaml_config_invalid_yaml(monkeypatch):
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

    # YAML config missing output to test env override
    yaml_content = """
usgs:
  monitoring_locations_url: "https://yaml-url.com"
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        config = load_config("dummy.yaml")

    # Env vars should override or fill missing keys
    assert config.monitoring_locations_url == "https://env-url.com"
    assert config.api_key == "secret-key"
    assert config.output_directory == "env_dir"
    assert config.filename_pattern == "env_file_{timestamp}.json"
    assert config.timestamp_format == "%H%M%S"

def test_load_config_yaml_only(monkeypatch):
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

    # No env vars set, so yaml values used
    assert config.monitoring_locations_url == "https://yaml-url.com"
    assert config.api_key is None
    assert config.output_directory == "yaml_dir"
    assert config.filename_pattern == "yaml_file_{timestamp}.json"
    assert config.timestamp_format == "%Y-%m-%d"

def test_load_config_defaults(monkeypatch):
    # No YAML, no env vars
    monkeypatch.delenv("API_URL", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("OUTPUT_DIR", raising=False)
    monkeypatch.delenv("FILENAME_PATTERN", raising=False)
    monkeypatch.delenv("TIMESTAMP_FORMAT", raising=False)

    config = load_config(None)
    assert config.monitoring_locations_url == "https://mock-url.com"
    assert config.api_key is None
    assert config.output_directory == "mock_data"
    assert config.filename_pattern == "usgs_monitoring_locations_{timestamp}.json"
    assert config.timestamp_format == "%Y%m%d_%H%M%S"

def test_config_validation_error(monkeypatch):
    # Provide invalid URL to force validation error
    yaml_content = """
usgs:
  monitoring_locations_url: 123  # Should be str, giving int to cause ValidationError
"""
    m = mock_open(read_data=yaml_content)
    with patch("builtins.open", m):
        with pytest.raises(ValidationError):
            load_config("dummy.yaml")
