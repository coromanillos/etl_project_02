###########################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Extract data from USGS REST API
# Endpoint: monitoring_locations
# Date: 08/02/25
###########################################

import logging
from typing import Optional, Protocol
import pandas as pd
from src.utils.serialization import save_dataframe_to_parquet

logger = logging.getLogger(__name__)

class HttpClient(Protocol):
    def get(self, url: str) -> dict:
        ...

def extract_usgs_monitoring_locations(url: str, client: HttpClient) -> Optional[pd.DataFrame]:
    if not url:
        logger.error("USGS monitoring_locations_url is missing.")
        return None

    try:
        logger.info(f"Fetching URL: {url}")
        response = client.get(url)
        features = response.get("features", [])
        if not features:
            logger.warning("No features found in USGS response")
            return None

        records = [
            {**feature.get("properties", {}), "id": feature["id"]}
            for feature in features
        ]
        return pd.DataFrame.from_records(records)

    except Exception as e:
        logger.exception(f"Failed to extract monitoring locations: {e}")
        return None
