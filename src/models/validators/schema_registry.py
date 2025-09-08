##########################################################################################
# models/schema_registry.py
# Description: Maps each endpoint to its Pandera validator schema
##########################################################################################

from validators.usgs_daily_validator_schema import daily_schema
from validators.usgs_monitoring_locations_validator_schema import monitoring_schema
from validators.usgs_parameter_codes_validator_schema import parameter_codes_schema

SCHEMA_REGISTRY = {
    "daily_values": daily_schema,
    "monitoring_locations": monitoring_schema,
    "parameter_codes": parameter_codes_schema,
}
