#####################################################################
# Name: extract_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Extract all USGS monitoring locations and save raw Parquet
# Date: 08/17/25
#####################################################################

import logging
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd

logger = logging.getLogger(__name__)

def extract_all_monitoring_locations(config):
    """
    Full extract of monitoring locations from the USGS API using pagination,
    then save each page to a timestamped Parquet file in the raw_data directory.
    """
    ml_config = config.usgs.monitoring_locations
    raw_data_dir = Path(config.paths.raw_data)
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    limit = ml_config.limit
    offset = 0
    max_retries = ml_config.max_retries
    timeout = ml_config.request_timeout
    base_url = ml_config.base_url 

    # Collect all records here
    all_records = []

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

        # Append all records 
        all_records.extend(features)

        logger.debug(f"Fetched {len(features)} records from offset {offset}")
        offset += limit

    # 🔹 Save all records to unified Parquet file
    if all_records:
        df = pd.DataFrame.from_records(all_records)
        timestamp = datetime.now().strftime(ml_config.timestamp_format)
        file_path = raw_data_dir / f"monitoring_locations_{timestamp}.parquet"
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved unified file with {len(df)} records to {file_path.name}")
    else:
        logger.warning("No records extracted; nothing to save.")

    logger.info("Full extraction complete")
