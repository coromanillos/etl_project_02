# conftest.py

import pytest
import pandas as pd
import logging

# Use a named logger (more compatible with Airflow and production-grade logging setups)
logger = logging.getLogger("usgs_test_logger")
logger.setLevel(logging.DEBUG)

# Configure named logger with handler and formatter only once
@pytest.fixture(scope="session")
def configure_test_logger():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

# Sample configuration mock
@pytest.fixture
def mock_config():
    return {
        "usgs": {
            "monitoring_locations_url": "https://example.com/api/mock"
        }
    }

# Fixture to simulate raw API response
@pytest.fixture
def sample_raw_usgs_data():
    return {
        "features": [
            {
                "id": "12345",
                "properties": {
                    "dec_lat_va": "38.8977",
                    "dec_long_va": "-77.0365",
                    "alt_va": "20.0",
                    "state_cd": "VA",
                    "county_cd": "51013",
                    "huc_cd": "02070010",
                    "agency_cd": "USGS",
                    "site_no": "01646500",
                    "station_nm": "Potomac River at Chain Bridge",
                    "inventory_dt": "2022-01-01"
                }
            }
        ]
    }

# Expected transformed data
@pytest.fixture
def expected_clean_dataframe():
    return pd.DataFrame([{
        "latitude": 38.8977,
        "longitude": -77.0365,
        "elevation_ft": 20.0,
        "state_code": "VA",
        "county_code": "51013",
        "huc_code": "02070010",
        "agency_code": "USGS",
        "site_number": "01646500",
        "station_name": "Potomac River at Chain Bridge",
        "inventory_date": pd.Timestamp("2022-01-01"),
        "id": "12345"
    }])
