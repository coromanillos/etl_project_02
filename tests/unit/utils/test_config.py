# tests/test_config.py

import os
import pytest
from src.utils.config import load_config

def test_config_loading(monkeypatch):
    monkeypatch.setenv("API_URL", "https://mock-url.com")
    monkeypatch.setenv("API_KEY", "mock-key")

    config = load_config("tests/test_config.yaml")

    assert config["api"]["url"] == "https://mock-url.com"
    assert config["api"]["key"] == "mock-key"
