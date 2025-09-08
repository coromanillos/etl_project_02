###########################################
# Name: base.py
# Author: Christopher O. Romanillos
# Description: Shared declarative base for all ORM models
###########################################

from sqlalchemy.orm import declarative_base

# Shared Base class across all models
Base = declarative_base()
