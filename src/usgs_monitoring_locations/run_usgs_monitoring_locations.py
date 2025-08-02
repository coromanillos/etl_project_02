###########################################
# Name: run_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: Wrapper script for DAG orchestration
# Date: 08/02/25
###########################################

import logging
import sys
from config import load_config
from extract_usgs_monitoring_locations import extract_usgs_monitoring_locations

def main() -> None:
    # Configure logging once here, at the top-level entrypoint
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        filepath = extract_usgs_monitoring_locations(config)
        if filepath:
            logger.info(f"Extraction completed successfully: {filepath}")
        else:
            logger.error("Extraction failed.")
    except Exception as e:
        logger.error(f"Fatal error in extraction script: {e}", exc_info=True)

if __name__ == "__main__":
    main()
