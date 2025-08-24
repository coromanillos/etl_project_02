#################################################################################
# Name: transform_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos 
# Description: Modular transform of USGS monitoring location data
# Date: 08/21/25
#################################################################################

from pathlib import Path
from typing import Optional
import pandas as pd

from src.utils.file_utils import get_latest_raw_file, load_parquet_file, save_parquet_file
from src.utils.dataframe_utils import cast_numeric_and_datetime
from src.exceptions import TransformError


# -----------------------------
# Domain-specific transformation
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


# -----------------------------
# Main transformation method
# -----------------------------
def transform_latest_monitoring_locations(config: dict, logger) -> Optional[Path]:
    """Full transformation pipeline for monitoring locations."""
    raw_data_dir = Path(config["paths"]["raw_data"])
    prepared_data_dir = Path(config["paths"]["prepared_data"])
    ml_config = config["usgs"]["monitoring_locations"]

    # Step 1: Get latest raw file
    latest_file = get_latest_raw_file(raw_data_dir, logger)

    # Step 2: Load raw parquet
    df_raw = load_parquet_file(latest_file, logger)

    # Step 3: Flatten features (domain-specific)
    df_flat = flatten_features_column(df_raw, logger)

    # Step 4: Cast schema
    df_casted = cast_numeric_and_datetime(
        df_flat,
        numeric_cols=["latitude", "longitude", "elevation_ft"],
        datetime_cols=["inventory_date"],
        logger=logger
    )

    # Step 5: Save prepared data
    prepared_file = save_parquet_file(
        df=df_casted,
        output_dir=prepared_data_dir,
        filename_prefix="monitoring_locations_prepared",
        timestamp_format=ml_config["timestamp_format"],
        logger=logger
    )

    logger.info("Full transformation complete")
    return prepared_file
