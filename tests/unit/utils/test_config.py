# tests/test_config.py

import os
from src.utils.config import get_config

def test_config_from_env(monkeypatch):
    # Set environment variables
    monkeypatch.setenv("MONITORING_LOCATIONS_URL", "http://example.com")
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_pass")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "test_db")

    config = get_config()

    assert config.monitoring_locations_url == "http://example.com"
    assert config.postgres_user == "test_user"
    assert config.postgres_password == "test_pass"
    assert config.postgres_host == "localhost"
    assert config.postgres_port == 5432
    assert config.postgres_db == "test_db"
