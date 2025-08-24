#################################################################################
# Name: file_utils.py
# Author: Christopher O. Romanillos
# Description: Generic file utilities for ETL pipelines
# Date: 08/21/25
#################################################################################

from pathlib import Path
import pandas as pd
from typing import Optional
from src.exceptions import SaveError, TransformError


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


def save_parquet_file(
    df: pd.DataFrame,
    output_dir: Path,
    filename_prefix: str,
    timestamp_format: str,
    logger
) -> Optional[Path]:
    """Save DataFrame to a Parquet file with timestamped filename."""
    if df.empty:
        logger.warning("No data to save")
        return None

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        from datetime import datetime
        timestamp = datetime.now().strftime(timestamp_format)
        file_path = output_dir / f"{filename_prefix}_{timestamp}.parquet"
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved {len(df)} records to {file_path.name}")
        return file_path
    except Exception as e:
        msg = f"Failed to save DataFrame: {e}"
        logger.error(msg)
        raise SaveError(msg)
