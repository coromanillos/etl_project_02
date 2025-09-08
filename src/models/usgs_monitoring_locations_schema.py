##################################################################
# Name: usgs_monitoring_locations_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema for USGS Monitoring Locations
##################################################################

from sqlalchemy import Column, String, Float, Boolean, Text, Index
from geoalchemy2 import Geometry
from .base import Base  

class MonitoringLocation(Base):
    __tablename__ = 'monitoring_locations'

    # Primary key
    id = Column(String(50), primary_key=True)  

    # Agency info
    agency_code = Column(String(5), nullable=False)
    agency_name = Column(String(50), nullable=True)

    # Location info
    monitoring_location_number = Column(Text, nullable=True)
    monitoring_location_name = Column(Text, nullable=True)
    district_code = Column(Text, nullable=True)
    country_code = Column(Text, nullable=True)
    country_name = Column(Text, nullable=True)
    state_code = Column(Text, nullable=True)
    state_name = Column(Text, nullable=True)
    county_code = Column(Text, nullable=True)
    county_name = Column(Text, nullable=True)
    minor_civil_division_code = Column(Text, nullable=True)

    # Site info
    site_type_code = Column(Text, nullable=True)
    site_type = Column(Text, nullable=True)
    hydrologic_unit_code = Column(Text, nullable=True)
    basin_code = Column(Text, nullable=True)

    # Altitude / depth
    altitude = Column(Float, nullable=True)
    altitude_accuracy = Column(Float, nullable=True)
    altitude_method_code = Column(Text, nullable=True)
    altitude_method_name = Column(Text, nullable=True)
    vertical_datum = Column(Text, nullable=True)
    vertical_datum_name = Column(Text, nullable=True)

    # Horizontal accuracy
    horizontal_positional_accuracy_code = Column(Text, nullable=True)
    horizontal_positional_accuracy = Column(Text, nullable=True)
    horizontal_position_method_code = Column(Text, nullable=True)
    horizontal_position_method_name = Column(Text, nullable=True)
    original_horizontal_datum = Column(Text, nullable=True)
    original_horizontal_datum_name = Column(Text, nullable=True)

    # Drainage areas
    drainage_area = Column(Float, nullable=True)
    contributing_drainage_area = Column(Float, nullable=True)

    # Time info
    time_zone_abbreviation = Column(Text, nullable=True)
    uses_daylight_savings = Column(Boolean, nullable=True)  # Converted from Y/N
    construction_date = Column(Text, nullable=True)         # Unknown format

    # Aquifer info
    aquifer_code = Column(Text, nullable=True)
    national_aquifer_code = Column(Text, nullable=True)
    aquifer_type_code = Column(Text, nullable=True)

    # Well info
    well_constructed_depth = Column(Float, nullable=True)
    hole_constructed_depth = Column(Float, nullable=True)
    depth_source_code = Column(Text, nullable=True)

    # Spatial info
    geometry = Column(Geometry("POINT", srid=4326), nullable=True)  # PostGIS Point

    # Table indexes
    __table_args__ = (
        Index('idx_state_code', 'state_code'),
        Index('idx_agency_code', 'agency_code'),
        Index('idx_geometry', 'geometry', postgresql_using='gist'),
    )
