#############################################################################
# Name: usgs_validator.py
# Author: Christopher O. Romanillos
# Description: Validation of transformed USGS API data against schema & rules
# Date: 09/19/25
#############################################################################

from typing import Dict, Any
import pandas as pd
import pandera as pa

from src.exceptions import ValidationError
from models.schema_registry import SCHEMA_REGISTRY


class USGSValidator:
    REQUIRED_KEYS = {"data_manager", "logger", "endpoint_config"}

    def __init__(self, **kwargs: Any):
        # Validate required keys
        missing = self.REQUIRED_KEYS - kwargs.keys()
        if missing:
            raise ValueError(f"Missing required arguments for USGSValidator: {missing}")

        self.data_manager = kwargs["data_manager"]
        self.logger = kwargs["logger"]
        self.endpoint_config = kwargs["endpoint_config"]

        # Allow extra kwargs for future extensibility
        self.__dict__.update(kwargs)

    ################################################
    # Data Loading
    ################################################
    def load_transformed_data(self, endpoint: str) -> pd.DataFrame:
        """Load latest transformed file from processed dir into a DataFrame."""
        try:
            df = self.data_manager.load_latest_file(
                endpoint, use_processed=True, as_dataframe=True
            )
            self.logger.info(
                f"Loaded transformed data for validation: {endpoint}, {len(df)} records"
            )
            return df
        except Exception as e:
            msg = f"Failed to load transformed file for endpoint '{endpoint}': {e}"
            self.logger.error(msg)
            raise ValidationError(msg)

    ################################################
    # Schema Validation
    ################################################
    def validate_schema(self, df: pd.DataFrame, endpoint: str) -> pd.DataFrame:
        """Check schema compliance using Pandera."""
        schema: pa.DataFrameSchema = SCHEMA_REGISTRY.get(endpoint)
        if not schema:
            raise ValidationError(
                f"No Pandera schema registered for endpoint '{endpoint}'"
            )

        try:
            validated_df = schema.validate(df, lazy=True)
            self.logger.info(f"Schema validation passed for endpoint: {endpoint}")
            return validated_df
        except pa.errors.SchemaErrors as e:
            self.logger.error(
                f"Schema validation failed for endpoint {endpoint}: {e.failure_cases}"
            )
            raise ValidationError("Schema validation failed")

    ################################################
    # Integrity Validation
    ################################################
    def validate_integrity(self, df: pd.DataFrame) -> None:
        """Check nulls in critical fields, duplicates, invalid values."""
        critical_fields = self.endpoint_config.get("critical_fields", [])
        for field in critical_fields:
            if df[field].isnull().any():
                self.logger.error(f"Null values found in critical field: {field}")
                raise ValidationError(f"Null values found in critical field: {field}")

        # Duplicates check
        id_field = self.endpoint_config.get("id_field", "id")
        if df[id_field].duplicated().any():
            self.logger.error(f"Duplicate IDs found in field: {id_field}")
            raise ValidationError(f"Duplicate IDs found in field: {id_field}")

        self.logger.info("Integrity validation passed")

    ################################################
    # Consistency Validation
    ################################################
    def validate_consistency(self, df: pd.DataFrame) -> None:
        """Cross-field consistency checks defined in config."""
        rules = self.endpoint_config.get("consistency_rules", [])
        for rule in rules:
            if rule == "geometry_requires_id":
                invalid = df[(df["geometry"].notnull()) & (df["id"].isnull())]
                if not invalid.empty:
                    self.logger.error("Geometry present without corresponding ID")
                    raise ValidationError("Geometry present without ID")

        self.logger.info("Consistency validation passed")

    ################################################
    # End-to-End Validation
    ################################################
    def validate_latest_file(self, endpoint: str) -> str:
        """End-to-end validation pipeline for latest transformed file."""
        try:
            df = self.load_transformed_data(endpoint)
            df = self.validate_schema(df, endpoint)
            self.validate_integrity(df)
            self.validate_consistency(df)
            self.logger.info(f"Validation completed successfully for {endpoint}")
            return f"Validation passed for {endpoint}"
        except ValidationError as e:
            msg = f"Validation failed for endpoint '{endpoint}': {e}"
            self.logger.error(msg)
            raise
