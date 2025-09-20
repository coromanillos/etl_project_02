# tests/unit/test_usgs_exporter.py

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.usgs_exporter import USGSExporter


def test_missing_required_keys_raises(base_export_config):
    """Constructor should raise ValueError if required kwargs are missing."""
    with pytest.raises(ValueError) as excinfo:
        USGSExporter(config=base_export_config)  # missing logger
    assert "Missing required arguments" in str(excinfo.value)


def test_make_db_url_builds_correctly(exporter):
    """Verify DB URL is constructed from config."""
    db_cfg = {
        "user": "alice",
        "password": "secret",
        "host": "db.example.com",
        "port": 6543,
        "database": "metrics",
    }
    url = exporter._make_db_url(db_cfg)
    assert url == "postgresql://alice:secret@db.example.com:6543/metrics"


def test_timestamped_path_creates_expected_filename(exporter):
    """Ensure _timestamped_path produces file under export_dir with suffix."""
    out = exporter._timestamped_path("my_export", ".csv")
    assert out.parent == exporter.export_dir
    assert out.name.startswith("my_export_")
    assert out.suffix == ".csv"


@patch("src.usgs_exporter.gpd.read_postgis")
def test_export_gis_success(mock_read_postgis, exporter, mock_logger):
    """_export_gis writes a GeoPackage and returns the output path."""
    mock_gdf = MagicMock()
    mock_read_postgis.return_value = mock_gdf
    mock_gdf.to_file = MagicMock()

    out = exporter._export_gis("test_view")

    # Functional assertions
    assert isinstance(out, Path)
    assert out.exists() is False
    mock_read_postgis.assert_called_once()
    mock_gdf.to_file.assert_called_once()

    # Log assertion
    mock_logger.info.assert_any_call(
        f"Exported GIS format for view=test_view → {out}"
    )


@patch("src.usgs_exporter.pd.read_sql")
def test_export_business_success(mock_read_sql, exporter, mock_logger):
    """_export_business writes a CSV and returns the output path."""
    mock_df = MagicMock()
    mock_read_sql.return_value = mock_df
    mock_df.to_csv = MagicMock()

    out = exporter._export_business("test_view")

    # Functional assertions
    assert isinstance(out, Path)
    assert out.suffix == ".csv"
    mock_read_sql.assert_called_once()
    mock_df.to_csv.assert_called_once()

    # Log assertion
    mock_logger.info.assert_any_call(
        f"Exported business format for view=test_view → {out}"
    )


def test_export_all_runs_all_formats(exporter, mock_logger):
    """export_all should iterate over configured exports and produce outputs."""
    exporter._export_gis = MagicMock(return_value=Path("fake.gpkg"))
    exporter._export_business = MagicMock(return_value=Path("fake.csv"))

    results = exporter.export_all()

    # Functional assertions
    assert "gis_export" in results
    assert "biz_export" in results
    assert results["gis_export"].name.endswith(".gpkg")
    assert results["biz_export"].name.endswith(".csv")

    # Log assertions
    mock_logger.info.assert_any_call("Running export: gis_export (gpkg)")
    mock_logger.info.assert_any_call("Running export: biz_export (csv)")


def test_export_all_unsupported_format_raises(exporter, mock_logger):
    """export_all should raise for unsupported formats."""
    exporter.export_config = {
        "bad_export": {"view_name": "test_view", "format": "xml"}
    }
    with pytest.raises(ValueError) as excinfo:
        exporter.export_all()
    assert "Unsupported export format" in str(excinfo.value)

    # Log assertion
    mock_logger.error.assert_called_once()
    assert "Unsupported export format" in mock_logger.error.call_args[0][0]
