###########################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Extract data from USGS REST API
# Endpoint: monitoring_locations
# Date: 08/02/25
###########################################

import os
import json
import logging
from datetime import datetime
import requests

logger = logging.getLogger(__name__)

def fetch_json(url: str) -> dict | None:
    """Fetch JSON data from the specified URL with error handling."""
    try:
        logger.debug(f"Fetching URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"HTTP request failed: {e}")
        return None

def extract_usgs_monitoring_locations(config: dict) -> str | None:
    """Extract USGS monitoring locations data and save to file."""
    url = config.get("usgs", {}).get("monitoring_locations_url")
    if not url:
        logger.error("Missing 'monitoring_locations_url' in config under 'usgs'")
        return None

    output_dir = config.get("output", {}).get("directory", "raw_data")
    timestamp_format = config.get("output", {}).get("timestamp_format", "%Y%m%d_%H%M%S")
    filename_pattern = config.get("output", {}).get("filename_pattern", "usgs_monitoring_locations_{timestamp}.json")

    # WARNING: No directory creation here, must ensure directory exists by environment or container setup

    timestamp = datetime.now().strftime(timestamp_format)
    filename = filename_pattern.format(timestamp=timestamp)
    filepath = os.path.join(output_dir, filename)

    data = fetch_json(url)
    if data is None:
        return None

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Extraction successful, data saved to: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to write data to file: {e}")
        return None
