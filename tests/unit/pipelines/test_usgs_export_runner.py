# tests/integration/pipelines/test_export_etl_runner.py

from unittest.mock import patch
from src.usgs_export_runner import run_usgs_exporter


def test_run_usgs_exporter_success(base_export_config, mock_logger):
    """Integration test for exporter runner using shared fixtures."""
    with patch(
        "src.usgs_exporter.USGSExporter.export_all",
        return_value={"gis_export": "/tmp/gis.gpkg"}
    ):
        result = run_usgs_exporter(config=base_export_config, logger=mock_logger)

    assert "gis_export" in result
    assert result["gis_export"] == "/tmp/gis.gpkg"
    mock_logger.info.assert_called()
