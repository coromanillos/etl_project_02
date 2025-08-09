import pytest
import pandas as pd
from sqlalchemy import create_engine

from src.usgs_monitoring_locations.load_usgs_monitoring_locations import load_usgs_monitoring_locations
from src.usgs_monitoring_locations.validate_usgs_monitoring_locations import validate_usgs_monitoring_locations

def make_df():
    return pd.DataFrame([{"site_number": "001", "latitude": 10.0, "longitude": 20.0}])

@pytest.fixture
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()

def test_load_success(monkeypatch, sqlite_engine):
    df = make_df()

    # Mock validation to pass (do nothing)
    monkeypatch.setattr(
        "src.usgs_monitoring_locations.validate_usgs_monitoring_locations.validate_usgs_monitoring_locations",
        lambda x: None
    )

    def session_factory():
        return sqlite_engine

    def metadata(engine):
        pass  # no-op for this test

    # Should not raise any errors now, real DB engine used
    load_usgs_monitoring_locations(df, session_factory, metadata)

def test_load_schema_validation_fails(monkeypatch, sqlite_engine):
    df = make_df()

    def fake_validate(_):
        raise ValueError("Invalid data")

    monkeypatch.setattr(
        "src.usgs_monitoring_locations.load_usgs_monitoring_locations.validate_usgs_monitoring_locations",
        fake_validate
    )

    def session_factory():
        return sqlite_engine

    def metadata(engine):
        pass

    with pytest.raises(ValueError, match="Invalid data"):
        load_usgs_monitoring_locations(df, session_factory, metadata)
