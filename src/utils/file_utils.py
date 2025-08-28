#################################################################################
# Name: file_utils.py
# Author: Christopher O. Romanillos
# Description: Generic file utilities for ETL pipelines with context object
# Date: 08/26/25
#################################################################################

from pathlib import Path
import pandas as pd
import json
from typing import Optional, List, Dict
from dataclasses import dataclass

from src.exceptions import SaveError, TransformError

"""
Methods could have been enclosed in a class to avoid having to
pass logger repetetively, but I think scripts having a single
focus is better than the convenience of having everything grouped
in the same relational Class. Also saves on creating similar 
wrappers across different classes.
"""

@dataclass
class FileContext:
    logger: any
    raw_data_dir: Path
    timestamp_format: str


def get_latest_raw_file(context: FileContext, filename_pattern: str) -> Path:
    """Locate the most recent timestamped Parquet file in raw_data."""
    files = list(context.raw_data_dir.glob(f"{filename_pattern}_*.parquet"))
    if not files:
        msg = f"No raw Parquet files found in {context.raw_data_dir}"
        context.logger.error(msg)
        raise FileNotFoundError(msg)
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    context.logger.info(f"Latest raw file found: {latest_file.name}")
    return latest_file


def load_parquet_file(context: FileContext, file_path: Path) -> pd.DataFrame:
    """Load a Parquet file into a DataFrame."""
    try:
        df = pd.read_parquet(file_path)
        context.logger.info(f"Loaded {len(df)} records from {file_path.name}")
        return df
    except Exception as e:
        msg = f"Failed to load Parquet file {file_path}: {e}"
        context.logger.error(msg)
        raise TransformError(msg)


def save_parquet_file(context: FileContext, df: pd.DataFrame, filename_prefix: str) -> Optional[Path]:
    """Save DataFrame to a Parquet file with timestamped filename."""
    if df.empty:
        context.logger.warning("No data to save")
        return None

    try:
        context.raw_data_dir.mkdir(parents=True, exist_ok=True)
        from datetime import datetime
        timestamp = datetime.now().strftime(context.timestamp_format)
        file_path = context.raw_data_dir / f"{filename_prefix}_{timestamp}.parquet"
        df.to_parquet(file_path, index=False)
        context.logger.info(f"Saved {len(df)} records to {file_path.name}")
        return file_path
    except Exception as e:
        msg = f"Failed to save DataFrame: {e}"
        context.logger.error(msg)
        raise SaveError(msg)


def save_json_file(context: FileContext, records: List[Dict], filename_prefix: str) -> Optional[Path]:
    """Save raw records to JSON file with timestamped filename."""
    if not records:
        context.logger.warning("No records to save")
        return None

    try:
        context.raw_data_dir.mkdir(parents=True, exist_ok=True)
        from datetime import datetime
        timestamp = datetime.utcnow().strftime(context.timestamp_format)
        file_path = context.raw_data_dir / f"{filename_prefix}_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        context.logger.info(f"Saved {len(records)} records to JSON: {file_path.name}")
        return file_path
    except Exception as e:
        msg = f"Failed to save JSON file: {e}"
        context.logger.error(msg)
        raise SaveError(msg)
