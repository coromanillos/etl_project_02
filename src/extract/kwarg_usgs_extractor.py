##########################################################################################
# Name: usgs_extractor.py
# Author: Christopher O. Romanillos
# Description: Class-based extraction of USGS API endpoints with validation*
# Date: 08/23/25
##########################################################################################

from typing import List, Dict, Optional
from urllib.parse import urlencode
from src.exceptions import ExtractionError


class USGSExtractor:
    REQUIRED_KEYS = {"config", "endpoint_key", "logger", "data_manager"}

    def __init__(self, **kwargs):
        """
        Generic extractor for any USGS API endpoint.

        Expected kwargs:
          - config (dict): Full project configuration dictionary
          - endpoint_key (str): Key in config['usgs']
          - logger: Logger instance
          - data_manager: DataManager (must be provided by runner or orchestrator)
          - http_client (optional): Custom HTTP client (defaults to requests)
        """

        # -------------------------------
        # Validation block for critical fields
        # -------------------------------
        missing = self.REQUIRED_KEYS - kwargs.keys()
        if missing:
            raise ValueError(f"Missing required arguments: {missing}")

        # Assign critical fields
        self.config: dict = kwargs["config"]
        self.endpoint_key: str = kwargs["endpoint_key"]
        self.logger = kwargs["logger"]
        self.data_manager = kwargs["data_manager"]

        # Assign optional fields with fallback
        self.http_client = kwargs.get("http_client") or __import__("requests")

        # Derived config values
        self.endpoint_config = self.config["usgs"][self.endpoint_key]
        self.file_type = self.endpoint_config.get("file_type", "json")

    ################################################
    # URL Helper
    ################################################
    def build_url(
        self, offset: Optional[int] = None, limit: Optional[int] = None, **kwargs
    ) -> str:
        """
        Construct a URL for the USGS API using config-driven parameters.
        Accepts arbitrary extra query params via kwargs.
        """
        query_params = {**self.endpoint_config.get("extra_params", {}), **kwargs}
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
                response = self.http_client.get(
                    url, timeout=self.endpoint_config["request_timeout"]
                )
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
    # Fetch all records (config-driven)
    ################################################
    def fetch_all_records(self) -> List[Dict]:
        """Fetch all records from the USGS API using pagination and mode from config"""
        mode = self.endpoint_config.get("mode", "full")
        if mode != "full":
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

        self.data_manager.save_file(
            data=all_records,
            endpoint=self.endpoint_key,
            file_type=self.file_type,
            use_processed=False,
        )
        return all_records

    ################################################
    # Fetch recent records (config-driven)
    ################################################
    def fetch_recent(self) -> List[Dict]:
        """Fetch recent records according to config['mode']"""
        limit = self.endpoint_config.get("limit", 10000)
        recent_period = self.endpoint_config.get("recent_period", "P1M")
        extra_params = self.endpoint_config.get("extra_params", {})

        query_params = {"datetime": recent_period, "limit": limit, **extra_params}
        url = self.build_url(**query_params)

        self.logger.info(f"Fetching recent data from {url}")
        data = self.fetch_with_retries(url)
        features = data.get("features", [])
        self.logger.info(f"Fetched {len(features)} recent records")

        self.data_manager.save_file(
            data=features,
            endpoint=self.endpoint_key,
            file_type=self.file_type,
            use_processed=False,
        )
        return features
