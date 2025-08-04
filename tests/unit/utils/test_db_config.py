# test_db_config.py

import urllib.parse
import pytest
from src.utils.db_config import get_postgres_url

def test_postgres_url_from_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ssword!")
    monkeypatch.setenv("POSTGRES_HOST", "db.example.com")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "mydb")

    expected = (
        "postgresql://user:"
        + urllib.parse.quote_plus("p@ssword!")
        + "@db.example.com:5433/mydb"
    )
    assert get_postgres_url() == expected

def test_postgres_url_with_defaults(monkeypatch):
    # Remove env vars to trigger defaults
    for var in [
        "POSTGRES_USER", "POSTGRES_PASSWORD", 
        "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB"
    ]:
        monkeypatch.delenv(var, raising=False)

    expected = "postgresql://postgres:postgres@localhost:5432/monitoring"
    assert get_postgres_url() == expected
