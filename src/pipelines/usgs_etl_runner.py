##########################################################################################
# Name: usgs_etl_runner.py
# Author: Christopher O. Romanillos
# Description: Reusable ETL runner for USGS endpoints (with archiving)
# Date: 09/12/25
##########################################################################################

from typing import Dict
from src.file_utils import DataManager
from src.usgs_extractor import USGSExtractor
from src.usgs_transformer import USGSTransformer
from src.usgs_validator import USGSValidator
from src.usgs_loader import USGSLoader
from src.usgs_archiver import USGSArchiver
from src.exceptions import ExtractionError, TransformError, ValidationError, LoaderError


def run_usgs_etl(
    endpoint_key: str,
    config: Dict,
    db_config: Dict,
    logger,
    include_geometry: bool = False,
    output_format: str = "csv"
) -> str:
    """
    Run full ETL pipeline for a single USGS endpoint.
    
    Steps:
        1. Extract from USGS API
        2. Transform extracted data
        3. Archive transformed data
        4. Validate transformed data
        5. Load validated data into PostGIS
    """

    try:
        # Initialize DataManager
        data_manager = DataManager(
            logger=logger,
            base_raw_dir=config["paths"]["raw_data"],
            base_processed_dir=config["paths"]["prepared_data"],
            timestamp_format=config["usgs"][endpoint_key].get("timestamp_format", "%Y-%m-%dT%H:%M:%S")
        )

        # Extract
        extractor = USGSExtractor(config=config, endpoint_key=endpoint_key, logger=logger, data_manager=data_manager)
        extractor.fetch_all_records()
        logger.info(f"Extraction completed for endpoint '{endpoint_key}'")

        # Transform
        transformer = USGSTransformer(data_manager=data_manager, logger=logger, endpoint_config=config["usgs"][endpoint_key])
        transformed_path = transformer.transform_latest_file(endpoint=endpoint_key, include_geometry=include_geometry, output_format=output_format)
        logger.info(f"Transformation completed for endpoint '{endpoint_key}': {transformed_path}")

        # Archive transformed file
        archiver = USGSArchiver(data_manager=data_manager, logger=logger, config=config)
        archived_path = archiver.archive_latest_file(endpoint=endpoint_key)
        if archived_path:
            logger.info(f"Archived transformed file for endpoint '{endpoint_key}': {archived_path}")
        else:
            logger.warning(f"No file archived for endpoint '{endpoint_key}'")

        # Validate
        validator = USGSValidator(data_manager=data_manager, logger=logger, endpoint_config=config["usgs"][endpoint_key])
        validator.validate_latest_file(endpoint=endpoint_key)
        logger.info(f"Validation completed for endpoint '{endpoint_key}'")

        # Load
        loader = USGSLoader(data_manager=data_manager, logger=logger, db_config=db_config, endpoint_config=config["usgs"][endpoint_key])
        result = loader.load_latest_file(endpoint=endpoint_key)
        logger.info(f"Load completed for endpoint '{endpoint_key}'")

        return f"ETL completed successfully for endpoint '{endpoint_key}': {result}"

    except (ExtractionError, TransformError, ValidationError, LoaderError) as e:
        logger.error(f"ETL failed for endpoint '{endpoint_key}': {e}")
        raise
