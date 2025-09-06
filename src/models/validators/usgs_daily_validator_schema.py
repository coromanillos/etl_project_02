###########################################
# Name: usgs_daily_validator_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema for USGS Daily Values
# Date: 09/01/25
###########################################

import pandera as pa
from pandera import Column, Check
from datetime import datetime

daily_schema = pa.DataFrameSchema({
    "uuid": Column(pa.String, nullable=False),
    "source_id": Column(pa.String, nullable=False),
    "time_series_id": Column(pa.String, nullable=False),
    "monitoring_location_id": Column(pa.String, nullable=False),
    "parameter_code": Column(pa.String, nullable=False),
    "statistic_id": Column(pa.String, nullable=False),
    "time": Column(pa.DateTime, nullable=False),
    "last_modified": Column(pa.DateTime, nullable=True),
    "value": Column(pa.String, nullable=True),
    "unit_of_measure": Column(pa.String, nullable=True),
    "approval_status": Column(pa.String, nullable=True),
    "qualifier": Column(pa.String, nullable=True),
    "geometry": Column(pa.String, nullable=True),  # WKT/GeoJSON representation
})
