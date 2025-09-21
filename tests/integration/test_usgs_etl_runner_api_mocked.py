# tests/integration/test_usgs_etl_runner_api_mocked.py

from unittest.mock import patch
from src.usgs_etl_runner import run_usgs_etl

def test_etl_pipeline_with_api_mocked(test_config, mock_logger):
    """
    Run the full ETL pipeline, mocking only the external API call.
    All downstream steps (transform, archive, validate, load) run on real data.
    """
    endpoint_key = "stations"
    db_config = test_config["db"]

    # Mock the API fetch only
    with patch(
        "src.usgs_extractor.requests.get"
    ) as mock_get:
        # Return a fake API JSON response
        mock_get.return_value.json.return_value = {
            "value": [
                {"id": 1, "geometry": None},
                {"id": 2, "geometry": None}
            ]
        }
        mock_get.return_value.status_code = 200

        # Run the ETL pipeline
        result = run_usgs_etl(
            endpoint_key=endpoint_key,
            config=test_config,
            db_config=db_config,
            logger=mock_logger,
            include_geometry=True,
            output_format="csv"
        )

    assert "ETL completed successfully" in result
    mock_logger.info.assert_called()
