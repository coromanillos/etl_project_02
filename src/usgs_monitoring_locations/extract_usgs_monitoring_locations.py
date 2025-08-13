###########################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos (Refactored by ChatGPT)
# Description: Stream USGS monitoring locations from API with per-page processing
# Date: 08/13/25
###########################################

import logging
from typing import Protocol, Dict, Generator
import pandas as pd

logger = logging.getLogger(__name__)

class HttpClient(Protocol):
    def get(self, url: str, params: dict = None) -> dict:
        ...

def extract_usgs_monitoring_locations_stream(
    url: str, client: HttpClient, limit: int = 10000
) -> Generator[pd.DataFrame, None, None]:
    """
    Stream paginated USGS monitoring locations data.

    Args:
        url: API endpoint
        client: HTTP client implementing .get()
        limit: number of records per request

    Yields:
        DataFrame for each page of results
    """
    if not url:
        logger.error("API URL missing for extraction")
        return

    offset = 0
    while True:
        params = {"f": "json", "limit": limit, "offset": offset}
        try:
            logger.info(f"Fetching records with offset={offset}")
            response = client.get(url, params=params)
            features = response.get("features", [])
            if not features:
                logger.info("No more data returned from API.")
                break

            records = [
                {**feature.get("properties", {}), "id": feature.get("id")}
                for feature in features
            ]
            yield pd.DataFrame.from_records(records)

            if len(features) < limit:
                break  # last page reached
            offset += limit

        except Exception as e:
            logger.exception(f"Failed extraction at offset {offset}: {e}")
            break
