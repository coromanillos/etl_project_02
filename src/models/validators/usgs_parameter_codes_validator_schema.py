############################################################
# Name: usgs_parameter_codes_validator_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema for USGS Parameter Codes
# Date: 09/02/25
#############################################################

import pandera as pa
from pandera import Column, Check

parameter_codes_schema = pa.DataFrameSchema({
    "id": Column(pa.String, nullable=False),
    "parameter_name": Column(pa.String, nullable=True),
    "unit_of_measure": Column(pa.String, nullable=True),
    "parameter_group_code": Column(pa.String, nullable=True),
    "parameter_description": Column(pa.String, nullable=True),
    "medium": Column(pa.String, nullable=True),
    "statistical_basis": Column(pa.String, nullable=True),
    "time_basis": Column(pa.String, nullable=True),
    "weight_basis": Column(pa.String, nullable=True),
    "particle_size_basis": Column(pa.String, nullable=True),
    "sample_fraction": Column(pa.String, nullable=True),
    "temperature_basis": Column(pa.String, nullable=True),
    "epa_equivalence": Column(pa.String, nullable=True),
    "geometry": Column(pa.String, nullable=True),
})
