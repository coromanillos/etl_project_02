# tests/integration/pipelines/test_usgs_etl_runner.py

from unittest.mock import patch
from src.usgs_etl_runner import run_usgs_etl


def test_run_usgs_etl_success(etl_config, db_config, mock_logger):
    """Integration test for ETL runner using shared fixtures."""
    endpoint = "stations"

    with patch("src.usgs_extractor.USGSExtractor.fetch_all_records", return_value=None), \
         patch("src.usgs_transformer.USGSTransformer.transform_latest_file", return_value="processed.csv"), \
         patch("src.usgs_archiver.USGSArchiver.archive_latest_file", return_value="archived.csv"), \
         patch("src.usgs_validator.USGSValidator.validate_latest_file", return_value=None), \
         patch("src.usgs_loader.USGSLoader.load_latest_file", return_value=42):

        result = run_usgs_etl(
            endpoint_key=endpoint,
            config=etl_config,
            db_config=db_config,
            logger=mock_logger
        )

    assert "ETL completed successfully" in result
    mock_logger.info.assert_called()
