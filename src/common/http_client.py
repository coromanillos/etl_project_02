###################################################
# Name: http_client.py
# Author: Christopher O. Romanillos
# Description: Wrapper script requests
# Date: 08/08/25
###################################################

import requests
from utils.logging_config import setup_logging
import logging

logger = logging.getLogger(__name__)

# This class wraps the HTTP GET requests in a reusable form
class RequestsHttpClient:
    def get(self, url: str, params=None, headers=None, timeout=10):
        logger.debug(f"Sending GET request to {url} with params={params} and timeout={timeout}")
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"HTTP error during GET request to {url}: {e}")
            raise
        logger.debug(f"Received response with status {response.status_code} from {url}")
        return response.json()
