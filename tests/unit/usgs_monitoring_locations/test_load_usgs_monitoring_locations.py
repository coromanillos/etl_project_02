# test_load_usgs_monitoring_locations.py

import pytest
from unittest.mock import patch, MagicMock
from src.load_usgs_monitoring_locations import load_usgs_monitoring_locations


@patch("src.load_usgs_monitoring_locations.deserialize_df")
@patch("src.load_usgs_monitoring_locations.create_engine")
@patch("src.load_usgs_monitoring_locations.sessionmaker")
@patch("src.load_usgs_monitoring_locations.monitoring_location_schema.validate")
def test_load_usgs_monitoring_locations_success(mock_validate, mock_sessionmaker, mock_engine, mock_deserialize):
    df_mock = MagicMock()
    df_mock.iterrows.return_value = [(0, MagicMock(to_dict=lambda: {"site_number": "00001"}))]
    mock_deserialize.return_value = df_mock

    mock_session = MagicMock()
    mock_sessionmaker.return_value = MagicMock(return_value=mock_session)

    blob = b"dummy_parquet_bytes"
    load_usgs_monitoring_locations(blob)

    mock_validate.assert_called_once()
    mock_session.return_value.bulk_save_objects.assert_called_once()
    mock_session.return_value.commit.assert_called_once()


@patch("src.load_usgs_monitoring_locations.deserialize_df", side_effect=Exception("Boom"))
def test_load_usgs_monitoring_locations_fail(mock_deserialize):
    with pytest.raises(Exception, match="Boom"):
        load_usgs_monitoring_locations(b"invalid_blob")
