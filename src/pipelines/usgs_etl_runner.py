##########################################################################################
# Name: usgs_etl_runner.py
# Author: Christopher O. Romanillos
# Description: Reusable ETL runner for USGS endpoints
# Date: 09/11/25
##########################################################################################

from typing import Dict
from src.file_utils import DataManager
from src.usgs_extractor import USGSExtractor
from src.usgs_transformer import USGSTransformer
from src.usgs_validator import USGSValidator
from src.usgs_loader import USGSLoader
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
        3. Validate transformed data
        4. Load validated data into PostGIS

    Args:
        endpoint_key (str): Key in config['usgs'] identifying the endpoint
        config (dict): Project configuration
        db_config (dict): Database connection config
        logger: Logger instance
        include_geometry (bool): Whether to include geometry in transformations
        output_format (str): Format to save transformed data (csv/parquet)

    Returns:
        str: Summary message of ETL result
    """

    try:
        # Initialize DataManager
        data_manager = DataManager(
            logger=logger,
            base_raw_dir=config["paths"]["raw_data"],
            base_processed_dir=config["paths"]["prepared_data"],
            timestamp_format=config["usgs"][endpoint_key].get("timestamp_format", "%Y-%m-%dT%H:%M:%S")
        )

        # 1️⃣ Extract
        extractor = USGSExtractor(config=config, endpoint_key=endpoint_key, logger=logger, data_manager=data_manager)
        extractor.fetch_all_records()
        logger.info(f"Extraction completed for endpoint '{endpoint_key}'")

        # 2️⃣ Transform
        transformer = USGSTransformer(data_manager=data_manager, logger=logger, endpoint_config=config["usgs"][endpoint_key])
        transformer.transform_latest_file(endpoint=endpoint_key, include_geometry=include_geometry, output_format=output_format)
        logger.info(f"Transformation completed for endpoint '{endpoint_key}'")

        # 3️⃣ Validate
        validator = USGSValidator(data_manager=data_manager, logger=logger, endpoint_config=config["usgs"][endpoint_key])
        validator.validate_latest_file(endpoint=endpoint_key)
        logger.info(f"Validation completed for endpoint '{endpoint_key}'")

        # 4️⃣ Load
        loader = USGSLoader(data_manager=data_manager, logger=logger, db_config=db_config, endpoint_config=config["usgs"][endpoint_key])
        result = loader.load_latest_file(endpoint=endpoint_key)
        logger.info(f"Load completed for endpoint '{endpoint_key}'")

        return f"ETL completed successfully for endpoint '{endpoint_key}': {result}"

    except (ExtractionError, TransformError, ValidationError, LoaderError) as e:
        logger.error(f"ETL failed for endpoint '{endpoint_key}': {e}")
        raise
