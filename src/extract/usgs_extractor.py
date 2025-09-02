##################################################################################
# Name: usgs_extractor.py
# Author: Christopher O. Romanillos
# Description: Class-based extraction of USGS API endpoints (generic)
#              focused solely on extraction tasks. File handling delegated to DataManager.
# Date: 08/23/25
##################################################################################

from typing import List, Dict, Optional
from urllib.parse import urlencode
from src.exceptions import ExtractionError
from src.file_utils import DataManager  # Ensure correct import path

class USGSExtractor:
    def __init__(
        self,
        config: dict,
        endpoint_key: str,
        logger,
        data_manager: Optional[DataManager] = None,
        http_client=None
    ):
        """
        Generic extractor for any USGS API endpoint.

        :param config: Full project configuration dictionary
        :param endpoint_key: Key in config['usgs']
        :param logger: Logger instance
        :param data_manager: Optional DataManager instance
        """
        self.config = config
        self.logger = logger
        self.http_client = http_client or __import__("requests")
        self.endpoint_config = config["usgs"][endpoint_key]
        self.endpoint_key = endpoint_key

        # Initialize DataManager if not provided
        self.data_manager = data_manager or DataManager(
            logger=logger,
            base_raw_dir=config["paths"]["raw_data"],
            base_processed_dir=config["paths"]["prepared_data"],
            timestamp_format=self.endpoint_config.get("timestamp_format", "%Y-%m-%dT%H:%M:%S")
        )

        # Default file type
        self.file_type = self.endpoint_config.get("file_type", "json")

    ################################################
    # URL Helper
    ################################################
    def build_url(self, extra_params: Optional[Dict] = None, offset: Optional[int] = None, limit: Optional[int] = None) -> str:
        """
        Construct a URL for the USGS API using config-driven parameters
        """
        query_params = self.endpoint_config.get("extra_params", {}).copy()
        if extra_params:
            query_params.update(extra_params)
        if limit is not None:
            query_params["limit"] = limit
        if offset is not None:
            query_params["offset"] = offset
        query_params["f"] = "json"
        return f"{self.endpoint_config['base_url']}&{urlencode(query_params)}"

    ################################################
    # Fetch with retries
    ################################################
    def fetch_with_retries(self, url: str) -> Dict:
        """Fetch JSON data from the USGS API with retry logic"""
        for attempt in range(self.endpoint_config["max_retries"]):
            try:
                response = self.http_client.get(url, timeout=self.endpoint_config["request_timeout"])
                response.raise_for_status()
                return response.json()
            except Exception as e:
                self.logger.warning(f"Attempt {attempt+1}/{self.endpoint_config['max_retries']} failed for {url}: {e}")
        raise ExtractionError(f"Failed to fetch data from {url} after {self.endpoint_config['max_retries']} retries")

    ################################################
    # Fetch all records (config-driven)
    ################################################
    def fetch_all_records(self) -> List[Dict]:
        """
        Fetch all records from the USGS API using pagination and mode from config
        Automatically saves data via DataManager
        """
        mode = self.endpoint_config.get("mode", "full")
        if mode != "full":
            # If not full mode, delegate to fetch_recent
            return self.fetch_recent()

        offset = 0
        limit = self.endpoint_config.get("limit", 10000)
        all_records: List[Dict] = []

        while True:
            url = self.build_url(offset=offset, limit=limit)
            self.logger.info(f"Fetching data: offset={offset}, limit={limit}")
            data = self.fetch_with_retries(url)
            features = data.get("features", [])
            if not features:
                self.logger.info("No more data returned; extraction complete")
                break
            all_records.extend(features)
            self.logger.debug(f"Fetched {len(features)} records from offset {offset}")
            offset += len(features)

        self.data_manager.save_file(data=all_records, endpoint=self.endpoint_key, file_type=self.file_type, use_processed=False)
        return all_records

    ################################################
    # Fetch recent records (config-driven)
    ################################################
    def fetch_recent(self) -> List[Dict]:
        """
        Fetch recent records according to config['mode'] (e.g., 'recent')
        Automatically saves data via DataManager
        """
        limit = self.endpoint_config.get("limit", 10000)
        recent_period = self.endpoint_config.get("recent_period", "P1M")  # default past month
        extra_params = self.endpoint_config.get("extra_params", {})

        query_params = {"datetime": recent_period, **extra_params, "limit": limit}
        url = self.build_url(extra_params=query_params)

        self.logger.info(f"Fetching recent data from {url}")
        data = self.fetch_with_retries(url)
        features = data.get("features", [])
        self.logger.info(f"Fetched {len(features)} recent records")

        self.data_manager.save_file(data=features, endpoint=self.endpoint_key, file_type=self.file_type, use_processed=False)
        return features
