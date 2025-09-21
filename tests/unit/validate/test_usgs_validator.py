# tests/unit/validate/test_usgs_validator.py

import pytest
import pandas as pd
from unittest.mock import MagicMock

from src.usgs_validator import USGSValidator
from src.exceptions import ValidationError


def test_load_transformed_data_success(validator, mock_data_manager, mock_logger):
    df = pd.DataFrame([{"id": 1, "name": "station"}])
    mock_data_manager.load_latest_file.return_value = df

    result = validator.load_transformed_data("stations")

    assert isinstance(result, pd.DataFrame)
    mock_logger.info.assert_called_once()


def test_load_transformed_data_failure(validator, mock_data_manager):
    mock_data_manager.load_latest_file.side_effect = Exception("read error")

    with pytest.raises(ValidationError):
        validator.load_transformed_data("stations")


def test_validate_schema_success(validator, patch_schema_registry, mock_logger):
    df = pd.DataFrame([{"id": 1, "name": "ok", "geometry": None}])

    result = validator.validate_schema(df, "stations")

    assert isinstance(result, pd.DataFrame)
    mock_logger.info.assert_any_call("Schema validation passed for endpoint: stations")


def test_validate_schema_failure(validator, patch_schema_registry):
    df = pd.DataFrame([{"id": "wrong_type", "name": 123, "geometry": None}])

    with pytest.raises(ValidationError):
        validator.validate_schema(df, "stations")


def test_validate_schema_no_schema(validator):
    df = pd.DataFrame([{"id": 1}])

    with pytest.raises(ValidationError):
        validator.validate_schema(df, "nope")


def test_validate_integrity_nulls_fail(validator):
    df = pd.DataFrame([{"id": None}])
    with pytest.raises(ValidationError):
        validator.validate_integrity(df)


def test_validate_integrity_duplicates_fail(validator):
    df = pd.DataFrame([{"id": 1}, {"id": 1}])
    with pytest.raises(ValidationError):
        validator.validate_integrity(df)


def test_validate_integrity_pass(validator, mock_logger):
    df = pd.DataFrame([{"id": 1}, {"id": 2}])
    validator.validate_integrity(df)
    mock_logger.info.assert_called_with("Integrity validation passed")


def test_validate_consistency_fail(validator):
    df = pd.DataFrame([{"id": None, "geometry": "POINT(0 0)"}])
    with pytest.raises(ValidationError):
        validator.validate_consistency(df)


def test_validate_consistency_pass(validator, mock_logger):
    df = pd.DataFrame([{"id": 1, "geometry": None}])
    validator.validate_consistency(df)
    mock_logger.info.assert_called_with("Consistency validation passed")


def test_validate_latest_file_success(validator, patch_schema_registry, mock_data_manager, mock_logger):
    df = pd.DataFrame([{"id": 1, "name": "ok", "geometry": None}])
    mock_data_manager.load_latest_file.return_value = df

    result = validator.validate_latest_file("stations")

    assert "Validation passed" in result
    mock_logger.info.assert_any_call("Validation completed successfully for stations")


def test_validate_latest_file_failure(validator, mock_data_manager):
    mock_data_manager.load_latest_file.side_effect = Exception("bad file")

    with pytest.raises(ValidationError):
        validator.validate_latest_file("stations")
