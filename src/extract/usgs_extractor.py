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
        :param endpoint_key: Key in config['usgs'] (e.g., 'monitoring_locations', 'daily_values', 'parameter_codes')
        :param logger: Logger instance
        :param data_manager: Optional DataManager instance for saving raw/processed data
        """
        self.config = config
        self.logger = logger
        self.http_client = http_client or __import__("requests")
        self.endpoint_config = config["usgs"][endpoint_key]
        self.endpoint_key = endpoint_key

        # Initialize DataManager if provided or create default
        self.data_manager = data_manager or DataManager(
            logger=logger,
            base_raw_dir=config["paths"]["raw_data"],
            base_processed_dir=config["paths"]["processed_data"],
            timestamp_format=self.endpoint_config.get("timestamp_format", "%Y-%m-%dT%H:%M:%S")
        )

    ################################################
    # URL Helper
    ################################################
    def build_url(self, offset: int, limit: int) -> str:
        """Construct a paginated USGS API URL."""
        return f"{self.endpoint_config['base_url']}&offset={offset}&limit={limit}"

    ################################################
    # Fetch with retries
    ################################################
    def fetch_with_retries(self, url: str) -> Dict:
        """Fetch JSON data from the USGS API with retry logic."""
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

    ################################################
    # Fetch all records
    ################################################
    def fetch_all_records(self, file_type: str = "json") -> List[Dict]:
        """
        Fetch all records from the USGS API using pagination.
        Automatically saves the data via DataManager.

        :param file_type: File type to save ("json" or "parquet")
        :return: List of records (features)
        """
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

        # Automatically save to raw_data
        self.data_manager.save_file(
            data=all_records,
            endpoint=self.endpoint_key,
            file_type=file_type,
            use_processed=False
        )

        return all_records

    ################################################
    # Fetch most recent month (incremental)
    ################################################
    def fetch_recent_month(self, extra_params: Optional[Dict] = None, file_type: str = "json") -> List[Dict]:
        """
        Fetch the most recent month of data from the USGS API.
        Automatically saves the data via DataManager.

        :param extra_params: Optional query parameters (e.g., site or parameter filters)
        :param file_type: File type to save ("json" or "parquet")
        :return: List of records (features)
        """
        query_params = {
            "datetime": "P1M",  # past month
            "limit": self.endpoint_config.get("limit", 10000),
            "f": "json"
        }
        if extra_params:
            query_params.update(extra_params)

        url = f"{self.endpoint_config['base_url']}&{urlencode(query_params)}"
        self.logger.info(f"Fetching most recent month of data from {url}")
        data = self.fetch_with_retries(url)
        features = data.get("features", [])
        self.logger.info(f"Fetched {len(features)} records for the past month")

        # Automatically save to raw_data
        self.data_manager.save_file(
            data=features,
            endpoint=self.endpoint_key,
            file_type=file_type,
            use_processed=False
        )

        return features
