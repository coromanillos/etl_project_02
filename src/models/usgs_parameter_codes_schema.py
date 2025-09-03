###########################################
# Name: usgs_parameter_codes_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema for USGS Parameter Codes
# Date: 09/02/25
###########################################

from sqlalchemy import Column, String, Text, Index
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class ParameterCode(Base):
    __tablename__ = 'parameter_codes'

    # Primary key
    id = Column(String(10), primary_key=True)  # Parameter code (unique identifier)

    # Core metadata
    parameter_name = Column(Text, nullable=True)  # Short name
    unit_of_measure = Column(Text, nullable=True)  # Reporting units
    parameter_group_code = Column(Text, nullable=True)  # Grouping for reporting
    parameter_description = Column(Text, nullable=True)  # Full description
    medium = Column(Text, nullable=True)  # Medium of parameter
    statistical_basis = Column(Text, nullable=True)  # Statistical basis
    time_basis = Column(Text, nullable=True)  # Time basis
    weight_basis = Column(Text, nullable=True)  # Weight basis
    particle_size_basis = Column(Text, nullable=True)  # Particle-size basis
    sample_fraction = Column(Text, nullable=True)  # Fraction type
    temperature_basis = Column(Text, nullable=True)  # Temperature basis
    epa_equivalence = Column(Text, nullable=True)  # USGS/EPA equivalence

    # Spatial info (optional)
    geometry = Column(Geometry("GEOMETRY", srid=4326), nullable=True)

    # Table indexes for performance
    __table_args__ = (
        Index('idx_parameter_group_code', 'parameter_group_code'),
        Index('idx_geometry', 'geometry', postgresql_using='gist'),
    )
