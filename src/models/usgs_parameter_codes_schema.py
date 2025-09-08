###########################################
# Name: usgs_parameter_codes_schema.py
# Author: Christopher O. Romanillos
# Description: SQLAlchemy ORM Schema for USGS Parameter Codes
# Date: 09/02/25
###########################################

from sqlalchemy import Column, String, Text, Index
from geoalchemy2 import Geometry
from .base import Base 

class ParameterCode(Base):
    __tablename__ = 'parameter_codes'

    # Primary key
    id = Column(String(10), primary_key=True)  # Parameter code (unique identifier)

    # Core metadata
    parameter_name = Column(String, nullable=True)  
    unit_of_measure = Column(String, nullable=True)  
    parameter_group_code = Column(String, nullable=True)  
    parameter_description = Column(String, nullable=True)  
    medium = Column(Text, nullable=True) 
    statistical_basis = Column(String, nullable=True)  
    time_basis = Column(String, nullable=True)  
    weight_basis = Column(String, nullable=True)  
    particle_size_basis = Column(String, nullable=True)  
    sample_fraction = Column(String, nullable=True)  
    temperature_basis = Column(String, nullable=True)  
    epa_equivalence = Column(String, nullable=True) 

    # Spatial info 
    geometry = Column(Geometry("GEOMETRY", srid=4326), nullable=True)

    # Indexes
    __table_args__ = (
        Index('idx_parameter_group_code', 'parameter_group_code'),
        Index('idx_geometry', 'geometry', postgresql_using='gist'),
    )
