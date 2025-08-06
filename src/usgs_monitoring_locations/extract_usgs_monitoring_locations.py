###########################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Extract data from USGS REST API
# Endpoint: monitoring_locations
# Date: 08/02/25
###########################################

import logging
from typing import Optional, Protocol

logger = logging.getLogger(__name__)

class HttpClient(Protocol):
    def get(self, url: str) -> dict:
        ...

def extract_usgs_monitoring_locations(config: dict, client: HttpClient) -> Optional[dict]:
    url = config.get("usgs", {}).get("monitoring_locations_url")
    if not url:
        logger.error("Missing 'monitoring_locations_url' in config under 'usgs'")
        return None
    try:
        logger.info(f"Fetching URL: {url}")
        response = client.get(url)
        logger.info("Fetch successful")
        return response
    except Exception as e:
        logger.exception("HTTP request failed")
        return None
