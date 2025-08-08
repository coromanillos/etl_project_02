###################################################
# Name: http_client.py
# Author: Christopher O. Romanillos
# Description: Wrapper script requests
# Date: 08/08/25
###################################################

import requests

# This class wraps the HTTP GET requests in a reusable form
class RequestsHttpClient:
    def get(self, url: str, params=None, headers=None, timeout=10):
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()
