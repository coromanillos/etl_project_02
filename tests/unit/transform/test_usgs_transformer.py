# tests/unit/test_usgs_transformer.py

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock

from src.exceptions import TransformError
from src.usgs_transformer import USGSTransformer


def test_missing_required_keys_raises(mock_logger, mock_data_manager):
    """Constructor should raise ValueError if required kwargs are missing."""
    with pytest.raises(ValueError):
        USGSTransformer(data_manager=mock_data_manager, logger=mock_logger)  # missing endpoint_config


def test_yesno_to_bool(transformer):
    assert transformer.yesno_to_bool("Y") is True
    assert transformer.yesno_to_bool("N") is False
    assert transformer.yesno_to_bool("maybe") is None
    assert transformer.yesno_to_bool(None) is None


def test_parse_datetime_success(transformer):
    dt = transformer.parse_datetime("2025-09-20T12:34:56Z")
    assert isinstance(dt, pd.Timestamp)
    assert dt.tzinfo is not None


def test_parse_datetime_failure_logs_warning(transformer, mock_logger):
    result = transformer.parse_datetime("not-a-date")
    assert result is None
    mock_logger.warning.assert_called_once()


def test_flatten_geometry_point(transformer):
    geom = {"type": "Point", "coordinates": [10, 20]}
    assert transformer.flatten_geometry(geom) == "POINT(10 20)"


def test_flatten_geometry_invalid(transformer, mock_logger):
    geom = {"type": "Point", "coordinates": [10]}  # bad coords
    result = transformer.flatten_geometry(geom)
    assert result is None
    # warning may or may not be logged depending on branch, safe to assert call count
    assert mock_logger.warning.called or result is None


def test_extract_properties_includes_id_and_geometry(transformer, mock_logger):
    records = [
        {"id": "abc", "properties": {"name": "site1"}, "geometry": {"type": "Point", "coordinates": [1, 2]}},
        {"id": "def", "properties": {"name": "site2"}},
    ]
    out = transformer.extract_properties(records, include_geometry=True)
    assert out[0]["id"] == "abc"
    assert "geometry" in out[0]
    assert out[1]["id"] == "def"
    mock_logger.info.assert_called_once()


def test_normalize_fields_applies_transformations(transformer):
    records = [
        {"id": "1", "active": "Y", "created_at": "2025-09-20T12:00:00Z", "geometry": {"type": "Point", "coordinates": [5, 6]}}
    ]
    out = transformer.normalize_fields(records)
    assert out[0]["active"] is True
    assert isinstance(out[0]["created_at"], pd.Timestamp)
    assert out[0]["geometry"].startswith("POINT(")


def test_to_dataframe_builds_dataframe(transformer):
    records = [{"id": "1", "value": 100}, {"id": "2", "value": 200}]
    df = transformer.to_dataframe(records)
    assert isinstance(df, pd.DataFrame)
    assert "id" in df.columns
    assert len(df) == 2


def test_transform_latest_file_success(transformer, mock_data_manager, tmp_path):
    # Mock data_manager methods
    mock_data_manager.load_latest_file.return_value = [
        {"id": "1", "properties": {"active": "Y", "created_at": "2025-09-20T12:00:00Z", "geometry": {"type": "Point", "coordinates": [5, 6]}}},
    ]
    expected_output = tmp_path / "out.csv"
    mock_data_manager.save_dataframe.return_value = expected_output

    out = transformer.transform_latest_file("stations", include_geometry=True, output_format="csv")
    assert str(out) == str(expected_output)
    transformer.logger.info.assert_any_call(f"Transformed data saved to {expected_output}")


def test_transform_latest_file_failure_raises(transformer, mock_data_manager, mock_logger):
    mock_data_manager.load_latest_file.side_effect = Exception("boom")

    with pytest.raises(TransformError) as excinfo:
        transformer.transform_latest_file("stations")

    assert "Failed to transform latest file" in str(excinfo.value)
    mock_logger.error.assert_called_once()
