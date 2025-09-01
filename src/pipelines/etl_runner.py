##################################################################################
# Name: etl_runner.py
# Author: Christopher O. Romanillos
# Description: Orchestrates extraction, transformation, and (future) validation/loading
#              of USGS API endpoints using reusable ETL classes.
# Date: 09/01/25
##################################################################################

from pathlib import Path
from src.config_loader import load_config
from src.logging_config import configure_logger
from src.file_utils import DataManager
from src.usgs_extractor import USGSExtractor
from src.usgs_transformer import USGSTransformer

# Placeholder imports for future validation and loading
# from src.usgs_validator import USGSValidator
# from src.usgs_loader import USGSLoader

def run_pipeline(endpoint_key: str, include_geometry: bool = False):
    """
    Generic ETL pipeline runner for a single USGS endpoint.

    Steps:
        1. Extract data (full or incremental depending on config)
        2. Transform extracted data
        3. Placeholder for validation (future)
        4. Placeholder for loading (future)
    """
    # Load config and logging
    config = load_config()
    logger = configure_logger(config.get("logging"))

    # Initialize DataManager
    dm = DataManager(
        logger=logger,
        base_raw_dir=Path(config["paths"]["raw_data"]),
        base_processed_dir=Path(config["paths"]["prepared_data"]),
        timestamp_format=config["usgs"][endpoint_key].get("timestamp_format", "%Y-%m-%dT%H:%M:%S")
    )

    # Initialize ETL classes
    extractor = USGSExtractor(config, endpoint_key, logger, data_manager=dm)
    transformer = USGSTransformer(dm, logger)
    # validator = USGSValidator(schema=config["usgs"][endpoint_key].get("schema"))
    # loader = USGSLoader(db_config=config["db"])

    # -----------------------------
    # Extraction
    # -----------------------------
    endpoint_mode = config["usgs"][endpoint_key]["mode"]
    extra_params = config["usgs"][endpoint_key].get("extra_params", {})

    if endpoint_mode == "full":
        raw_data = extractor.fetch_all_records()
    else:
        raw_data = extractor.fetch_recent_month(extra_params=extra_params)

    # -----------------------------
    # Transformation
    # -----------------------------
    transformed_path = transformer.transform_latest_file(endpoint_key, include_geometry=include_geometry)
    logger.info(f"Transformed data saved to {transformed_path}")

    # -----------------------------
    # Validation (placeholder)
    # -----------------------------
    # validator.validate(transformed_path)
    # logger.info(f"Validated transformed data for {endpoint_key}")

    # -----------------------------
    # Loading (placeholder)
    # -----------------------------
    # loader.load(transformed_path)
    # logger.info(f"Loaded transformed data for {endpoint_key}")

    logger.info(f"Pipeline completed successfully for endpoint '{endpoint_key}'")
    return transformed_path

# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    endpoints = ["monitoring_locations", "daily_values", "parameter_codes"]
    for ep in endpoints:
        run_pipeline(ep, include_geometry=True)
