import pytest
import pandas as pd
from unittest.mock import MagicMock
from sqlalchemy.engine import Engine

from src.usgs_monitoring_locations.load_usgs_monitoring_locations import load_usgs_monitoring_locations
from src.usgs_monitoring_locations.validate_usgs_monitoring_locations import validate_usgs_monitoring_locations

def make_df():
    return pd.DataFrame([{"site_number": "001", "latitude": 10.0, "longitude": 20.0}])

def test_load_success(monkeypatch):
    df = make_df()

    # Mock validation to pass
    monkeypatch.setattr("src.usgs_monitoring_locations.validate_usgs_monitoring_locations.validate_usgs_monitoring_locations", lambda x: None)

    # Mock engine and to_sql
    mock_engine = MagicMock(spec=Engine)
    monkeypatch.setattr(df, "to_sql", lambda *args, **kwargs: None)

    def session_factory():
        return mock_engine

    def metadata(engine):
        pass

    # Should not raise any errors
    load_usgs_monitoring_locations(df, session_factory, metadata)

def test_load_schema_validation_fails(monkeypatch):
    df = make_df()

    # Simulate validation failure
    def fake_validate(_):
        raise ValueError("Invalid data")

    monkeypatch.setattr("src.usgs_monitoring_locations.validate_usgs_monitoring_locations.validate_usgs_monitoring_locations", fake_validate)

    def session_factory():
        return MagicMock(spec=Engine)

    def metadata(engine):
        pass

    with pytest.raises(ValueError, match="Invalid data"):
        load_usgs_monitoring_locations(df, session_factory, metadata)
