##################################################################################
# Name: usgs_extractor.py
# Author: Christopher O. Romanillos
# Description: Class-based extraction of USGS API endpoints (generic)
# Date: 08/23/25
##################################################################################

from pathlib import Path
from typing import List, Dict

from src.exceptions import ExtractionError


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

"""
Allows for passing a dictionary that adds extra API parameters.
Maybe config functionality? I think its fine as is.
"""
    #######################################################
    # Fetch most recent month of data (incremental extract)
    #######################################################
    def fetch_recent_month(self, extra_params: Dict = None) -> List[Dict]:
        """
        Fetch the most recent month of data from the USGS API.

        :param extra_params: Optional query parameters to include (e.g., monitoring_location_id, parameter_code).
        :return: List of records (features) for the most recent month.
        """
        # Base query: always bound to the past month
        query_params = {
            "datetime": "P1M",  # past month
            "limit": self.endpoint_config.get("limit", 10000),
            "f": "json"
        }

        # Merge in any extra parameters supplied
        if extra_params:
            query_params.update(extra_params)

        # Build full URL
        base_url = self.endpoint_config["base_url"]
        url = f"{base_url}&{urlencode(query_params)}"

        self.logger.info(f"Fetching most recent month of data from {url}")
        data = self.fetch_with_retries(url)

        # Features are where the actual records live
        features = data.get("features", [])
        self.logger.info(f"Fetched {len(features)} records for the past month")

        return features
