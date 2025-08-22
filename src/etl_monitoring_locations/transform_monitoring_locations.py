#################################################################################
# Name: transform_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos 
# Description: Modular transform of USGS monitoring location data
# Date: 08/21/25
#################################################################################

from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd

from src.exceptions import TransformError, SaveError

# -----------------------------
# File Helpers
# -----------------------------
def get_latest_raw_file(raw_data_dir: Path, logger) -> Path:
    """Locate the most recent timestamped Parquet file in raw_data."""
    files = list(raw_data_dir.glob("monitoring_locations_*.parquet"))
    if not files:
        msg = f"No raw Parquet files found in {raw_data_dir}"
        logger.error(msg)
        raise FileNotFoundError(msg)
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    logger.info(f"Latest raw file found: {latest_file.name}")
    return latest_file


def load_parquet_file(file_path: Path, logger) -> pd.DataFrame:
    """Load a Parquet file into a DataFrame."""
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded {len(df)} records from {file_path.name}")
        return df
    except Exception as e:
        msg = f"Failed to load Parquet file {file_path}: {e}"
        logger.error(msg)
        raise TransformError(msg)


# -----------------------------
# Transformation Helpers
# -----------------------------
def flatten_features_column(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Flatten the 'features' column (if present) into tabular structure.
    """
    if "features" not in df.columns:
        logger.debug("'features' column not found, assuming already flattened")
        return df

    flattened_records = []
    for feature in df["features"].tolist():
        record = feature.get("properties", {}).copy()
        record["id"] = feature.get("id")
        record["geometry"] = feature.get("geometry")
        flattened_records.append(record)

    logger.info(f"Flattened {len(flattened_records)} feature records")
    return pd.DataFrame.from_records(flattened_records)


def cast_numeric_and_datetime(df: pd.DataFrame, logger) -> pd.DataFrame:
    """Cast numeric and datetime fields into correct formats."""
    for col in ["latitude", "longitude", "elevation_ft"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            logger.debug(f"Converted column {col} to numeric")

    if "inventory_date" in df.columns:
        df["inventory_date"] = pd.to_datetime(df["inventory_date"], errors="coerce")
        logger.debug("Converted inventory_date to datetime")

    return df


# -----------------------------
# Data Storage
# -----------------------------
def save_prepared_data(
    df: pd.DataFrame,
    prepared_data_dir: Path,
    timestamp_format: str,
    logger
) -> Optional[Path]:
    """Save prepared DataFrame to Parquet with timestamped filename."""
    if df.empty:
        logger.warning("No data to save after transformation")
        return None

    try:
        prepared_data_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime(timestamp_format)
        file_path = prepared_data_dir / f"monitoring_locations_prepared_{timestamp}.parquet"
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved prepared data to {file_path.name}")
        return file_path
    except Exception as e:
        msg = f"Failed to save prepared data: {e}"
        logger.error(msg)
        raise SaveError(msg)


# -----------------------------
# Main transformation method
# -----------------------------
def transform_latest_monitoring_locations(config: dict, logger) -> Optional[Path]:
    """Full transformation pipeline for monitoring locations."""
    raw_data_dir = Path(config["paths"]["raw_data"])
    prepared_data_dir = Path(config["paths"]["prepared_data"])
    ml_config = config["usgs"]["monitoring_locations"]

    latest_file = get_latest_raw_file(raw_data_dir, logger)
    df_raw = load_parquet_file(latest_file, logger)

    df_flat = flatten_features_column(df_raw, logger)
    df_casted = cast_numeric_and_datetime(df_flat, logger)

    prepared_file = save_prepared_data(
        df=df_casted,
        prepared_data_dir=prepared_data_dir,
        timestamp_format=ml_config["timestamp_format"],
        logger=logger
    )

    logger.info("Full transformation complete")
    return prepared_file
