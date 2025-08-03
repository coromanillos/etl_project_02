###########################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Extract data from USGS REST API
# Endpoint: monitoring_locations
# Date: 08/02/25
###########################################

import logging
import requests

logger = logging.getLogger(__name__)

def fetch_json(url: str) -> dict | None:
    """Fetch JSON data from the specified URL with error handling."""
    try:
        logger.info(f"Fetching URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        logger.info("Fetch successful")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"HTTP request failed: {e}")
        return None

def extract_usgs_monitoring_locations(config: dict) -> dict | None:
    """
    Extract USGS monitoring locations JSON data.

    Args:
        config: dict with config including key 'usgs.monitoring_locations_url'

    Returns:
        Raw JSON data dict or None on failure.
    """
    url = config.get("usgs", {}).get("monitoring_locations_url")
    if not url:
        logger.error("Missing 'monitoring_locations_url' in config under 'usgs'")
        return None

    data = fetch_json(url)
    return data
