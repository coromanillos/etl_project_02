###########################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Extract data from USGS REST API
# Endpoint: monitoring_locations
# Date: 08/02/25
###########################################

import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

def fetch_json(url: str) -> Optional[dict]:
    try:
        logger.info(f"Fetching URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        logger.info("Fetch successful")
        return response.json()
    except requests.RequestException as e:
        logger.exception("HTTP request failed")
        return None

def extract_usgs_monitoring_locations(config: dict) -> Optional[dict]:
    url = config.get("usgs", {}).get("monitoring_locations_url")
    if not url:
        logger.error("Missing 'monitoring_locations_url' in config under 'usgs'")
        return None
    return fetch_json(url)
