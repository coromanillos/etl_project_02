##################################################################################
# Name: extract_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Modular extraction of USGS monitoring locations
# Date: 08/18/25
##################################################################################

from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd

from src.exceptions import ExtractionError, SaveError

# -----------------------------
# URL and API Helpers
# -----------------------------
def build_usgs_url(base_url: str, offset: int, limit: int) -> str:
    """Construct a paginated USGS API URL."""
    return f"{base_url}&offset={offset}&limit={limit}"


def fetch_with_retries(
    url: str,
    max_retries: int,
    timeout: int,
    logger,
    http_client=None
) -> Dict:
    """Fetch JSON data with retry logic."""
    client = http_client or __import__("requests")
    for attempt in range(max_retries):
        try:
            response = client.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{max_retries} failed for {url}: {e}")
    raise ExtractionError(f"Failed to fetch data from {url} after {max_retries} retries")


# -----------------------------
# Data Extraction
# -----------------------------
def fetch_all_records(
    base_url: str,
    limit: int,
    max_retries: int,
    timeout: int,
    logger,
    http_client=None
) -> List[Dict]:
    """Fetch all records from the USGS API using pagination."""
    offset = 0
    all_records: List[Dict] = []

    while True:
        url = build_usgs_url(base_url, offset, limit)
        logger.info(f"Fetching data: offset={offset}, limit={limit}")
        data = fetch_with_retries(url, max_retries, timeout, logger, http_client=http_client)
        features = data.get("features", [])
        if not features:
            logger.info("No more data returned; extraction complete")
            break
        all_records.extend(features)
        logger.debug(f"Fetched {len(features)} records from offset {offset}")
        offset += len(features)

    return all_records


# -----------------------------
# Data Storage
# -----------------------------
def save_records_to_parquet(
    records: List[Dict],
    raw_data_dir: Path,
    timestamp_format: str,
    logger
) -> Optional[Path]:
    """Save records to a timestamped Parquet file."""
    if not records:
        logger.warning("No records to save")
        return None

    try:
        df = pd.DataFrame.from_records(records)
        timestamp = datetime.now().strftime(timestamp_format)
        file_path = raw_data_dir / f"monitoring_locations_{timestamp}.parquet"

        raw_data_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved {len(df)} records to {file_path}")
        return file_path
    except Exception as e:
        raise SaveError(f"Failed to save records to {file_path}: {e}")


# -----------------------------
# Main extraction method
# -----------------------------
def extract_all_monitoring_locations(config: dict, logger, http_client=None) -> Optional[Path]:
    """Full extraction pipeline for monitoring locations."""
    ml_config = config["usgs"]["monitoring_locations"]
    raw_data_dir = Path(config["paths"]["raw_data"])

    all_records = fetch_all_records(
        base_url=ml_config["base_url"],
        limit=ml_config["limit"],
        max_retries=ml_config["max_retries"],
        timeout=ml_config["request_timeout"],
        logger=logger,
        http_client=http_client
    )

    file_path = save_records_to_parquet(
        records=all_records,
        raw_data_dir=raw_data_dir,
        timestamp_format=ml_config["timestamp_format"],
        logger=logger
    )

    logger.info("Full extraction complete")
    return file_path
