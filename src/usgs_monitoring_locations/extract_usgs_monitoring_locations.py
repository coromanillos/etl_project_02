###########################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Extract data from USGS REST API 
# Endpoint: monitoring_locations
# Data: 08/02/25
###########################################

import requests
import json
from datetime import datetime
import os

# Define the USGS endpoint with format=json
USGS_MONITORING_LOCATIONS_URL = "https://labs.waterdata.usgs.gov/api/collections/monitoring-locations/items?f=json"

# Create output directory if not exists
output_dir = "raw_data"
os.makedirs(output_dir, exist_ok=True)

# Timestamped filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"usgs_monitoring_locations_{timestamp}.json"
filepath = os.path.join(output_dir, filename)

def extract_usgs_monitoring_locations():
    try:
        response = requests.get(USGS_MONITORING_LOCATIONS_URL)
        response.raise_for_status()
        data = response.json()

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"[SUCCESS] Data extracted and saved to: {filepath}")

    except requests.RequestException as e:
        print(f"[ERROR] Failed to retrieve data: {e}")

if __name__ == "__main__":
    extract_usgs_monitoring_locations()
