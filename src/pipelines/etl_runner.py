#################################################################################
# Name: etl_runner.py
# Author: Christopher O. Romanillos
# Description: Generic entrypoint module to orchestrate extraction of any USGS
#              endpoint for use in DAG scheduling (Airflow, Prefect, etc.)
# Date: 08/27/25
#################################################################################

import pandas as pd
from pathlib import Path

# Project imports
from utils.config_loader import load_config
from utils.logging_config import configure_logger
from utils.file_utils import FileContext, save_parquet_file
from utils.usgs_extractor import USGSExtractor
from src.exceptions import ExtractionError, SaveError


def run_extraction(endpoint_key: str) -> Path:
    """
    Orchestrates the extraction for a given USGS endpoint.
    Reads extraction mode and extra parameters from config.

    Args:
        endpoint_key: str
            Key in config['usgs'] (e.g., 'monitoring_locations', 'daily_values', 'parameter_codes')

    Returns:
        Path to saved parquet file.
    """

    # ---------------------------
    # Load configuration + logger
    # ---------------------------
    config = load_config()
    logger = configure_logger(config.get("logging"))

    # ---------------------------
    # Initialize file context
    # ---------------------------
    context = FileContext(
        logger=logger,
        raw_data_dir=Path(config["paths"]["raw_data"]),
        timestamp_format=config.get("timestamp_format", "%Y%m%d_%H%M%S"),
    )

    # ---------------------------
    # Extraction Phase
    # ---------------------------
    endpoint_config = config["usgs"].get(endpoint_key, {})
    mode = endpoint_config.get("mode", "full")  # default: full extract
    extra_params = endpoint_config.get("extra_params", {})

    extractor = USGSExtractor(
        config=config,
        endpoint_key=endpoint_key,
        logger=logger,
    )

    try:
        if mode == "full":
            records = extractor.fetch_all_records()
        elif mode == "recent":
            records = extractor.fetch_recent_month(extra_params=extra_params)
        else:
            raise ValueError(f"Unsupported extraction mode: {mode}")

        logger.info(f"Total {endpoint_key} records fetched: {len(records)}")
    except ExtractionError as e:
        logger.error(f"Extraction failed for {endpoint_key}: {e}")
        raise

    # ---------------------------
    # Transform to DataFrame
    # ---------------------------
    try:
        df = pd.json_normalize(records)
        logger.info(f"Normalized {endpoint_key} into DataFrame with {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        logger.error(f"Failed to normalize records for {endpoint_key}: {e}")
        raise

    # ---------------------------
    # Save as Parquet
    # ---------------------------
    try:
        saved_file = save_parquet_file(
            context=context,
            df=df,
            filename_prefix=endpoint_key
        )
        if saved_file:
            logger.info(f"Extraction for {endpoint_key} completed. File saved: {saved_file}")
        return saved_file
    except SaveError as e:
        logger.error(f"Failed to save extracted data for {endpoint_key}: {e}")
        raise
