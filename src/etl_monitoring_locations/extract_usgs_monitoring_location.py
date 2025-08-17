#####################################################################
# Name: extract_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Extract all USGS monitoring locations and save raw JSON
# Date: 08/16/25
#####################################################################

import logging
from pathlib import Path
from datetime import datetime
import json
import requests

logger = logging.getLogger(__name__)

def extract_all_monitoring_locations(config):
    """
    Full extract fo  monitoring locations from the USGS API using pagination,
    then save each page to a timestamped JSON file in the raw_data directory.
    """
    ml_cfg = config.usgs.monitoring_locations
    raw_data_dir = Path(config.paths.raw_data)
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    limit = ml_cfg.limit
    offset = 0
    max_retries = ml_cfg.max_retries
    timeout = ml_cfg.request_timeout
    base_url = ml_cfg.base_url 

    while True:
        url = f"{base_url}&offset={offset}&limit={limit}"
        logger.info(f"Fetching data: offset={offset}, limit={limit}")

        # Retry logic
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                data = response.json()
                break
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed for offset {offset}: {e}")
        else:
            raise Exception(f"Failed to fetch data from {url} after {max_retries} retries")

        # Check to see if the data response is empty, if so, break out of the loop
        features = data.get("features", [])
        if not features:
            logger.info("No more data returned; extraction complete")
            break

        # Save raw data to file with timestamp
        timestamp = datetime.now().strftime(ml_cfg.timestamp_format)
        file_path = raw_data_dir / f"monitoring_locations_{timestamp}_offset{offset}.json"
        with open(file_path, "w") as f:
            json.dump(data, f)
        logger.info(f"Saved {len(features)} records to {file_path.name}")

        offset += limit

    logger.info("Full extraction complete")
