# tests/unit/utils/test_config_loader.py

import os
import pytest
import yaml
from pathlib import Path

from src.config_loader import get_env_config_path, validate_config, load_config


# ----------------------------
# get_env_config_path tests
# ----------------------------

def test_get_env_config_path_dev(monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    path = get_env_config_path()
    assert path.name == "config-dev.yaml"

def test_get_env_config_path_invalid(monkeypatch):
    monkeypatch.setenv("APP_ENV", "staging")
    with pytest.raises(ValueError, match="Invalid APP_ENV"):
        get_env_config_path()


# ----------------------------
# validate_config tests
# ----------------------------

def test_validate_config_valid(sample_config_dict, sample_config_schema):
    # Should not raise
    validate_config(sample_config_dict, sample_config_schema)

def test_validate_config_missing_key(sample_config_dict, sample_config_schema):
    bad_config = {"db": {"host": "localhost"}}  # missing port
    with pytest.raises(ValueError, match="Missing required section: 'port'"):
        validate_config(bad_config["db"], sample_config_schema["db"])

def test_validate_config_type_mismatch(sample_config_schema):
    bad_config = {"db": {"host": "localhost", "port": "not-a-number"}}
    with pytest.raises(ValueError, match="must be of type int"):
        validate_config(bad_config, sample_config_schema)


# ----------------------------
# load_config tests (integration)
# ----------------------------

def test_load_config_valid(monkeypatch, make_temp_config_file, sample_config_dict, sample_config_schema):
    # Create temporary config file
    config_file = make_temp_config_file("config-test.yaml", sample_config_dict)

    # Patch APP_ENV and schema
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setattr("src.config_loader.get_config_schema", lambda: sample_config_schema)

    # Monkeypatch project root resolution
    monkeypatch.setattr("src.config_loader.Path.resolve", lambda self: config_file.parent.parent / "src/config_loader.py")

    # Create fake config directory
    config_dir = config_file.parent
    monkeypatch.setattr("src.config_loader.Path.parent", config_file.parent.parent)

    # Run
    config = load_config()
    assert config["db"]["host"] == "localhost"
    assert config["db"]["port"] == 5432
