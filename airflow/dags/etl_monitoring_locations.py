###########################################
# Name: dag_extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Airflow DAG to fully extract USGS monitoring locations
# Date: 08/16/25
###########################################

from datetime import datetime, timedelta
from pathlib import Path
import json
import requests
import yaml
import logging

from airflow import DAG
from airflow.decorators import task

# --------------------------
# Load configuration
# --------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

RAW_DATA_DIR = Path(config["paths"]["raw_data"])
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

USGS_URL = config["usgs"]["monitoring_locations_url"]
LIMIT = 10000
TIMEOUT = config["usgs"].get("request_timeout", 10)
MAX_RETRIES = config["usgs"].get("max_retries", 3)

logger = logging.getLogger(__name__)

# --------------------------
# DAG Definition
# --------------------------
default_args = {
    "owner": "cromanillos",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="extract_usgs_monitoring_locations",
    default_args=default_args,
    description="Full extract of USGS monitoring locations API",
    start_date=datetime(2025, 8, 16),
    schedule_interval="@quarterly",  # or adjust as needed
    catchup=False,
    tags=["usgs", "extract", "raw_data"],
)

# --------------------------
# HTTP Client with retries
# --------------------------
def fetch_page(url: str, params: dict):
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
    raise Exception(f"Failed to fetch data from {url} after {MAX_RETRIES} retries.")

# --------------------------
# Task: extract one page
# --------------------------
@task(dag=dag)
def extract_page(offset: int):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    params = {"f": "json", "limit": LIMIT, "offset": offset}

    data = fetch_page(USGS_URL, params)

    features = data.get("features", [])
    if not features:
        logger.info(f"No data returned at offset {offset}")
        return 0  # indicate no records

    file_path = RAW_DATA_DIR / f"monitoring_locations_{timestamp}_offset{offset}.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    logger.info(f"Saved {len(features)} records to {file_path.name}")

    return len(features)

# --------------------------
# Task: determine offsets
# --------------------------
@task(dag=dag)
def compute_offsets():
    """Dynamically compute offsets by checking the first page size."""
    first_page = fetch_page(USGS_URL, {"f": "json", "limit": LIMIT, "offset": 0})
    total_records = first_page.get("numberMatched", 0)  # if API provides total count
    if not total_records:
        # fallback: assume unknown, loop until empty
        total_records = 100000  # max guess
    offsets = list(range(0, total_records, LIMIT))
    return offsets

# --------------------------
# DAG Flow
# --------------------------
offsets = compute_offsets()
for offset in offsets:
    extract_page(offset)
