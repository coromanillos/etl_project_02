##########################################################
# Name: usgs_daily_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema for USGS Daily Values
##########################################################

from sqlalchemy import Column, String, Text, Date, DateTime, Index, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import text
from geoalchemy2 import Geometry
from .base import Base  

class DailyValue(Base):
    __tablename__ = 'daily_values'

    # Surrogate key
    uuid = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))

    # Source-provided ID (volatile)
    source_id = Column(String(50), nullable=False, index=True)

    # Foreign key references
    time_series_id = Column(String(50), nullable=False)
    monitoring_location_id = Column(
        String(50), 
        ForeignKey('monitoring_locations.id', ondelete='CASCADE'), 
        nullable=False
    )
    parameter_code = Column(
        String(50), 
        ForeignKey('parameter_codes.id', ondelete='CASCADE'), 
        nullable=False
    )
    statistic_id = Column(String(50), nullable=False)

    # Temporal
    time = Column(Date, nullable=False)
    last_modified = Column(DateTime, nullable=True)

    # Observed value
    value = Column(Text, nullable=True)
    unit_of_measure = Column(Text, nullable=True)
    approval_status = Column(Text, nullable=True)
    qualifier = Column(Text, nullable=True)

    # Spatial info
    geometry = Column(Geometry("POINT", srid=4326), nullable=True)

    # Indexes
    __table_args__ = (
        Index('idx_time_series_id', 'time_series_id'),
        Index('idx_monitoring_location_id', 'monitoring_location_id'),
        Index('idx_parameter_code', 'parameter_code'),
        Index('idx_geometry', 'geometry', postgresql_using='gist'),
    )
