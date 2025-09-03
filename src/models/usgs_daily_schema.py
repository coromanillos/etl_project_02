###########################################
# Name: usgs_daily_values_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema for USGS Daily Values
# Date: 09/01/25
###########################################

from sqlalchemy import Column, String, Text, Float, DateTime, Index
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class DailyValue(Base):
    __tablename__ = 'daily_values'

    # Primary key
    id = Column(String(50), primary_key=True)  # UUID

    # Foreign key references / identifiers
    time_series_id = Column(String(50), nullable=False)
    monitoring_location_id = Column(String(50), nullable=False)
    parameter_code = Column(String(50), nullable=False)
    statistic_id = Column(String(50), nullable=False)

    # Temporal (store as DateTime after transformation)
    time = Column(DateTime, nullable=False)
    last_modified = Column(DateTime, nullable=True)

    # Observed value
    value = Column(Text, nullable=True)  # transmitted as string to preserve precision
    unit_of_measure = Column(Text, nullable=True)
    approval_status = Column(Text, nullable=True)  
    qualifier = Column(Text, nullable=True)       

    # Spatial info
    geometry = Column(Geometry("POINT", srid=4326), nullable=True)

    # Table indexes for faster queries
    __table_args__ = (
        Index('idx_time_series_id', 'time_series_id'),
        Index('idx_monitoring_location_id', 'monitoring_location_id'),
        Index('idx_parameter_code', 'parameter_code'),
        Index('idx_geometry', 'geometry', postgresql_using='gist'),
    )
