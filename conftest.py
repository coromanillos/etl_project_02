#conftest.py
import sys
import os
import logging
import pytest
import pandas as pd

# Add the absolute path to 'src' directory to sys.path for test imports
SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "src"))
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# Named logger for tests
logger = logging.getLogger("usgs_test_logger")
logger.setLevel(logging.DEBUG)

@pytest.fixture(scope="session")
def configure_test_logger():
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

@pytest.fixture
def mock_config():
    return {
        "usgs": {
            "monitoring_locations_url": "https://example.com/api/mock"
        }
    }

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
