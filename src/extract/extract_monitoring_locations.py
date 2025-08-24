##################################################################################
# Name: usgs_extractor.py
# Author: Christopher O. Romanillos
# Description: Class-based extraction of USGS API endpoints (generic)
# Date: 08/23/25
##################################################################################

from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from src.exceptions import ExtractionError, SaveError


class USGSExtractor:
    def __init__(self, config: dict, endpoint_key: str, logger, http_client=None):
        """
        Generic extractor for any USGS API endpoint.
        
        :param config: Full project configuration dictionary
        :param endpoint_key: Key in config['usgs'] (e.g., 'monitoring_locations', 'daily_values', 'parameter_codes')
        """
        self.config = config
        self.logger = logger
        self.http_client = http_client or __import__("requests")

        self.endpoint_config = config["usgs"][endpoint_key]
        self.raw_data_dir = Path(config["paths"]["raw_data"])
        self.endpoint_key = endpoint_key

    # -----------------------------
    # URL Helper
    # -----------------------------
    def build_url(self, offset: int, limit: int) -> str:
        """Construct a paginated USGS API URL."""
        return f"{self.endpoint_config['base_url']}&offset={offset}&limit={limit}"

    # -----------------------------
    # Fetch with retries
    # -----------------------------
    def fetch_with_retries(self, url: str) -> Dict:
        """Fetch JSON data with retry logic."""
        for attempt in range(self.endpoint_config["max_retries"]):
            try:
                response = self.http_client.get(url, timeout=self.endpoint_config["request_timeout"])
                response.raise_for_status()
                return response.json()
            except Exception as e:
                self.logger.warning(
                    f"Attempt {attempt+1}/{self.endpoint_config['max_retries']} failed for {url}: {e}"
                )
        raise ExtractionError(
            f"Failed to fetch data from {url} after {self.endpoint_config['max_retries']} retries"
        )

    # -----------------------------
    # Fetch all records
    # -----------------------------
    def fetch_all_records(self) -> List[Dict]:
        """Fetch all records from the USGS API using pagination."""
        offset = 0
        all_records: List[Dict] = []

        while True:
            url = self.build_url(offset, self.endpoint_config["limit"])
            self.logger.info(f"Fetching data: offset={offset}, limit={self.endpoint_config['limit']}")
            data = self.fetch_with_retries(url)
            features = data.get("features", [])
            if not features:
                self.logger.info("No more data returned; extraction complete")
                break
            all_records.extend(features)
            self.logger.debug(f"Fetched {len(features)} records from offset {offset}")
            offset += len(features)

        return all_records

    # -----------------------------
    # Save to Parquet
    # -----------------------------
    def save_to_parquet(self, records: List[Dict]) -> Optional[Path]:
        """Save records to a timestamped Parquet file."""
        if not records:
            self.logger.warning("No records to save")
            return None

        try:
            df = pd.DataFrame.from_records(records)
            timestamp = datetime.now().strftime(self.endpoint_config["timestamp_format"])
            file_path = self.raw_data_dir / f"{self.endpoint_key}_{timestamp}.parquet"

            self.raw_data_dir.mkdir(parents=True, exist_ok=True)
            df.to_parquet(file_path, index=False)
            self.logger.info(f"Saved {len(df)} records to {file_path}")
            return file_path
        except Exception as e:
            raise SaveError(f"Failed to save records to {file_path}: {e}")

    # -----------------------------
    # Full extraction pipeline
    # -----------------------------
    def extract_all(self) -> Optional[Path]:
        """Run the full extraction pipeline."""
        all_records = self.fetch_all_records()
        file_path = self.save_to_parquet(all_records)
        self.logger.info("Full extraction complete")
        return file_path
