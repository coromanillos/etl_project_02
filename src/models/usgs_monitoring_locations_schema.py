###########################################
# Name: usgs_monitoring_locations_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema scipt 
# Date: 08/02/25
###########################################

from sqlalchemy import Column, String, Float, Boolean, Date, Text
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class MonitoringLocation(Base):
    __tablename__ = 'monitoring_locations'

    id = Column(String(15), primary_key=True)  # e.g., "USGS-02238500"
    agency_code = Column(String(100), nullable=False)
    agency_name = Column(String)
    monitoring_location_number = Column(String)
    monitoring_location_name = Column(String)

    district_code = Column(String)
    country_code = Column(String)
    country_name = Column(String)
    state_code = Column(String)
    state_name = Column(String)
    county_code = Column(String)
    county_name = Column(String)
    minor_civil_division_code = Column(String)

    site_type_code = Column(String)
    site_type = Column(String)
    hydrologic_unit_code = Column(String)
    basin_code = Column(String)

    altitude = Column(Float)
    altitude_accuracy = Column(Float)
    altitude_method_code = Column(String)
    altitude_method_name = Column(String)
    vertical_datum = Column(String)
    vertical_datum_name = Column(String)

    horizontal_positional_accuracy_code = Column(String)
    horizontal_positional_accuracy = Column(String)
    horizontal_position_method_code = Column(String)
    horizontal_position_method_name = Column(String)
    original_horizontal_datum = Column(String)
    original_horizontal_datum_name = Column(String)

    drainage_area = Column(Float)
    contributing_drainage_area = Column(Float)

    time_zone_abbreviation = Column(String)
    uses_daylight_savings = Column(String)
    construction_date = Column(String)

    aquifer_code = Column(String)
    national_aquifer_code = Column(String)
    aquifer_type_code = Column(String)

    well_constructed_depth = Column(Float)
    hole_constructed_depth = Column(Float)
    depth_source_code = Column(String)

    geometry = Column(Geometry("POINT", srid=4326))  # Uses PostGIS, so use Geometry type from geoalchemy2
