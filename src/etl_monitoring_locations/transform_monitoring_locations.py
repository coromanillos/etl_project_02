#################################################################################
# Name: transform_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos 
# Description: Transform USGS monitoring location data per page for streaming ETL
# Date: 08/17/25
#################################################################################

import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

# Under consideration for moving to utils/ directory
def get_latest_raw_file(raw_data_dir: Path) -> Path:
    """
    Locate the most recent timestamped Parquet file in raw_data.
    """
    files = list(raw_data_dir.glob("monitoring_locations_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No raw Parquet files found in {raw_data_dir}")
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    return latest_file

# Under consideration for moving to utils/ directory
def flatten_features(df: pd.DataFrame, features: list) -> pd.DataFrame:
    """
    Flatten the features list from raw JSON structure into a DataFrame
    including 'id' and 'geometry' fields.
    """
    flattened_records = []
    for feature in features:
        record = feature.get("properties", {}).copy()
        record["id"] = feature.get("id")
        record["geometry"] = feature.get("geometry")
        flattened_records.append(record)
    return pd.DataFrame.from_records(flattened_records)

def transform_latest_monitoring_locations(config):
    """
    Transform the latest extracted monitoring locations Parquet
    and save to prepared_data directory as timestamped Parquet.
    """
    raw_data_dir = Path(config.paths.raw_data)
    prepared_data_dir = Path(config.paths.prepared_data)
    prepared_data_dir.mkdir(parents=True, exist_ok=True)

    latest_file = get_latest_raw_file(raw_data_dir)
    logger.info(f"Transforming latest raw data file: {latest_file.name}")

    # Load the raw Parquet
    df_raw = pd.read_parquet(latest_file)

    # If original JSON had 'features', flatten them
    if "features" in df_raw.columns:
        df_flat = flatten_features(df_raw["features"].tolist(), df_raw["features"])
    else:
        df_flat = df_raw  # already flattened

    # Convert numeric and datetime columns if needed
    for col in ["latitude", "longitude", "elevation_ft"]:
        if col in df_flat.columns:
            df_flat[col] = pd.to_numeric(df_flat[col], errors="coerce")
    if "inventory_date" in df_flat.columns:
        df_flat["inventory_date"] = pd.to_datetime(df_flat["inventory_date"], errors="coerce")

    # Save prepared data with timestamp
    timestamp = datetime.now().strftime(config.usgs.monitoring_locations.timestamp_format)
    prepared_file = prepared_data_dir / f"monitoring_locations_prepared_{timestamp}.parquet"
    df_flat.to_parquet(prepared_file, index=False)
    logger.info(f"Saved prepared data to {prepared_file.name}")
