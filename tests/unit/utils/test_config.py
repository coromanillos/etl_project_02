# tests/unit/utils/test_config.py

import textwrap
import pytest
from pydantic import ValidationError
from src.utils.config import load_config, load_yaml_config, Config, DBConfig

@pytest.fixture
def env_vars(monkeypatch):
    """Set environment variables for Config, including nested DB fields."""
    monkeypatch.setenv("API_URL", "http://example.com/api")
    monkeypatch.setenv("OUTPUT_DIR", "/tmp/output")
    monkeypatch.setenv("FILENAME_PATTERN", "file_{date}.txt")
    monkeypatch.setenv("TIMESTAMP_FORMAT", "%Y-%m-%d")

    # Nested DB config vars using double underscore separator
    monkeypatch.setenv("DB__USER", "testuser")
    monkeypatch.setenv("DB__PASSWORD", "testpass")
    monkeypatch.setenv("DB__HOST", "localhost")
    monkeypatch.setenv("DB__PORT", "5432")  # keep as string; Pydantic converts to int
    monkeypatch.setenv("DB__DATABASE", "testdb")

def test_load_yaml_config_reads_file(tmp_path):
    yaml_content = textwrap.dedent("""
        usgs:
          monitoring_locations_url: "http://example.com/yaml"
        output:
          directory: "/data"
          filename_pattern: "out_{date}.csv"
          timestamp_format: "%Y%m%d"
        db:
          user: "yamluser"
          password: "yamlpass"
          host: "yamlhost"
          port: 1234
          database: "yamldb"
    """)
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(yaml_content)

    config_data = load_yaml_config(str(yaml_file))
    assert config_data["usgs"]["monitoring_locations_url"] == "http://example.com/yaml"
    assert config_data["db"]["user"] == "yamluser"
    assert config_data["db"]["port"] == 1234

def test_load_config_merges_yaml_and_env(env_vars, tmp_path):
    yaml_content = textwrap.dedent("""
        usgs:
          monitoring_locations_url: "http://yaml-override.com"
        output:
          directory: "/yaml/output"
        db:
          user: "yamluser"
    """)
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(yaml_content)

    config = load_config(str(yaml_file))

    # YAML should override env vars
    assert config.monitoring_locations_url == "http://yaml-override.com"
    assert config.output_directory == "/yaml/output"
    assert config.db.user == "yamluser"

    # Fallback to env vars for missing YAML fields
    assert config.filename_pattern == "file_{date}.txt"
    assert config.db.password == "testpass"

def test_load_config_from_env_only(env_vars):
    config = load_config(None)

    assert config.monitoring_locations_url == "http://example.com/api"
    assert config.output_directory == "/tmp/output"
    assert config.filename_pattern == "file_{date}.txt"
    assert config.db.user == "testuser"
    assert config.db.database == "testdb"

def test_load_config_validation_error(monkeypatch):
    monkeypatch.delenv("OUTPUT_DIR", raising=False)
    monkeypatch.delenv("FILENAME_PATTERN", raising=False)
    monkeypatch.delenv("TIMESTAMP_FORMAT", raising=False)
    monkeypatch.delenv("DB__USER", raising=False)
    monkeypatch.delenv("DB__PASSWORD", raising=False)
    monkeypatch.delenv("DB__HOST", raising=False)
    monkeypatch.delenv("DB__PORT", raising=False)
    monkeypatch.delenv("DB__DATABASE", raising=False)

    # Set only one required env var
    monkeypatch.setenv("API_URL", "http://example.com/api")

    with pytest.raises(ValidationError):
        load_config(None)
