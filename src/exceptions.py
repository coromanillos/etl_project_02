"""
Centralize exceptions for repeated scenarios
 Implemented for repeated error scenarios as a
 result of multiple ETL from different API
 endpoints. 

 Scalability concern:If more pipelines are added, group them 
 logically (e.g., ETLErrors, ConfigErrors) so they don’t all 
 live in one giant file...
"""

class ExtractionError(Exception):
    """Raised when data extraction from an external source fails after retries."""
    pass

class SaveError(Exception):
    """Raised when saving data to storage fails."""
    pass

class TransformError(Exception):
    """Raised when data transformation from a raw source fails"""
    pass

class ValidationError(Exception):
    """Raised when validation of transformed data fails."""
    pass


class LoaderError(Exception):
    """Raised when loading data into the database fails."""
    pass