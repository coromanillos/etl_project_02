##################################################################################
# Name: usgs_extractor.py
# Author: Christopher O. Romanillos
# Description: Class-based extraction of USGS API endpoints (generic)
# Date: 08/23/25
##################################################################################

from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from src.exceptions import ExtractionError
from src.utils.file_utils import save_parquet_file  


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

    ################################################
    # URL Helper; Local use only by Extraction phase
    ################################################
    def build_url(self, offset: int, limit: int) -> str:
        """Construct a paginated USGS API URL."""
        return f"{self.endpoint_config['base_url']}&offset={offset}&limit={limit}"

    #########################################################
    # Fetch with retries; Local use only by Extraction phase.
    #########################################################
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

    ###################
    # Fetch all records
    ###################
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

    ################################
    # Save to Parquet (thin wrapper)
    ################################
    """
    I am familiar with Parquet being a more efficient file format for storage and 
    analytics within cloud providers, AWS/Azure and their data lakes, but I do not 
    want to risk potential downstream errors and incompatibility concerns between 
    parquet, postgreSQL and postGIS, so I think we will pivot to using json primarily 
    """
    def save_to_parquet(self, records: List[Dict]) -> Optional[Path]:
        """Thin wrapper around save_parquet_file utility."""
        if not records:
            self.logger.warning("No records to save")
            return None

        df = pd.DataFrame.from_records(records)
        return save_parquet_file(
            df=df,
            output_dir=self.raw_data_dir,
            filename_prefix=self.endpoint_key,
            timestamp_format=self.endpoint_config["timestamp_format"],
            logger=self.logger,
        )

    # -----------------------------
    # Full extraction pipeline
    # -----------------------------
    def extract_all(self) -> Optional[Path]:
        """Run the full extraction pipeline."""
        all_records = self.fetch_all_records()
        file_path = self.save_to_parquet(all_records)
        self.logger.info("Full extraction complete")
        return file_path
