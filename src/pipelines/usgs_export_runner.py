################################################################################
# Name: usgs_export_runner.py
# Author: Christopher O. Romanillos
# Description: Export runner wrapper for USGS views
# Date: 09/13/25
################################################################################

from src.usgs_exporter import USGSExporter
from typing import Dict
from logging import Logger

def run_usgs_exporter(config: Dict, logger: Logger):
    """
    Wrapper for USGSExporter:
    Accepts config and logger from the DAG for consistency.
    """
    exporter = USGSExporter(config=config, logger=logger)
    results = exporter.export_all()
    for export_name, path in results.items():
        logger.info(f"Export completed: {export_name} → {path}")
    return results
