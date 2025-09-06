###########################################
# Name: usgs_monitoring_locations_validator_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema for USGS Monitoring Locations
###########################################

import pandera as pa
from pandera import Column, Check

monitoring_schema = pa.DataFrameSchema({
    "id": Column(pa.String, nullable=False),
    "agency_code": Column(pa.String, nullable=False),
    "agency_name": Column(pa.String, nullable=True),
    "monitoring_location_number": Column(pa.String, nullable=True),
    "monitoring_location_name": Column(pa.String, nullable=True),
    "district_code": Column(pa.String, nullable=True),
    "country_code": Column(pa.String, nullable=True),
    "country_name": Column(pa.String, nullable=True),
    "state_code": Column(pa.String, nullable=True),
    "state_name": Column(pa.String, nullable=True),
    "county_code": Column(pa.String, nullable=True),
    "county_name": Column(pa.String, nullable=True),
    "minor_civil_division_code": Column(pa.String, nullable=True),
    "site_type_code": Column(pa.String, nullable=True),
    "site_type": Column(pa.String, nullable=True),
    "hydrologic_unit_code": Column(pa.String, nullable=True),
    "basin_code": Column(pa.String, nullable=True),
    "altitude": Column(pa.Float, nullable=True),
    "altitude_accuracy": Column(pa.Float, nullable=True),
    "altitude_method_code": Column(pa.String, nullable=True),
    "altitude_method_name": Column(pa.String, nullable=True),
    "vertical_datum": Column(pa.String, nullable=True),
    "vertical_datum_name": Column(pa.String, nullable=True),
    "horizontal_positional_accuracy_code": Column(pa.String, nullable=True),
    "horizontal_positional_accuracy": Column(pa.String, nullable=True),
    "horizontal_position_method_code": Column(pa.String, nullable=True),
    "horizontal_position_method_name": Column(pa.String, nullable=True),
    "original_horizontal_datum": Column(pa.String, nullable=True),
    "original_horizontal_datum_name": Column(pa.String, nullable=True),
    "drainage_area": Column(pa.Float, nullable=True),
    "contributing_drainage_area": Column(pa.Float, nullable=True),
    "time_zone_abbreviation": Column(pa.String, nullable=True),
    "uses_daylight_savings": Column(pa.Bool, nullable=True),
    "construction_date": Column(pa.String, nullable=True),
    "aquifer_code": Column(pa.String, nullable=True),
    "national_aquifer_code": Column(pa.String, nullable=True),
    "aquifer_type_code": Column(pa.String, nullable=True),
    "well_constructed_depth": Column(pa.Float, nullable=True),
    "hole_constructed_depth": Column(pa.Float, nullable=True),
    "depth_source_code": Column(pa.String, nullable=True),
    "geometry": Column(pa.String, nullable=True),
})
