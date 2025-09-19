##################################################################################
# Name: usgs_transformer.py
# Author: Christopher O. Romanillos
# Description: Class-based transformation of USGS API raw data 
# Date: 09/18/25
##################################################################################

from typing import List, Dict, Any
import pandas as pd
from datetime import datetime

from src.exceptions import TransformError


class USGSTransformer:
    REQUIRED_KEYS = {"data_manager", "logger", "endpoint_config"}

    def __init__(self, **kwargs: Any):

        # Validate required keys
        missing = self.REQUIRED_KEYS - kwargs.keys()
        if missing:
            raise ValueError(f"Missing required arguments: {missing}")
            
        self.data_manager = kwargs["data_manager"]
        self.logger = kwargs["logger"]
        self.endpoint_config = kwargs["endpoint_config"]

        # Optional attributes can still come through kwargs if needed later
        self.__dict__.update(kwargs)

        # Map of built-in transformation functions
        self.transformation_funcs = {
            "yesno_to_bool": self.yesno_to_bool,
            "parse_datetime": self.parse_datetime,
            "flatten_geometry": self.flatten_geometry,
        }

    ################################################
    # Extraction Helpers
    ################################################
    def extract_properties(self, records: List[Dict], include_geometry: bool = False) -> List[Dict]:
        extracted = []
        for feature in records:
            props = feature.get("properties", {}) or {}
            if "id" in feature:
                props["id"] = feature["id"]
            if include_geometry and feature.get("geometry") is not None:
                props["geometry"] = feature["geometry"]
            extracted.append(props)
        self.logger.info(
            f"Extracted {len(extracted)} property records "
            f"(with id{' and geometry' if include_geometry else ''})"
        )
        return extracted

    ################################################
    # Transformation Functions
    ################################################
    def yesno_to_bool(self, value):
        if value is None:
            return None
        v = str(value).strip().upper()
        if v == "Y":
            return True
        if v == "N":
            return False
        return None

    def parse_datetime(self, value):
        if value is None:
            return None
        try:
            return pd.to_datetime(value, utc=True)
        except Exception:
            self.logger.warning(f"Failed to parse datetime: {value}")
            return None

    def flatten_geometry(self, value):
        """
        Converts geometry dict to WKT string for database storage.
        Returns None if geometry is missing or invalid.
        """
        if value is None:
            return None
        try:
            geom_type = value.get("type", "").upper()
            coords = value.get("coordinates")
            if geom_type == "POINT" and coords and len(coords) == 2:
                return f"POINT({coords[0]} {coords[1]})"
        except Exception:
            self.logger.warning(f"Failed to flatten geometry: {value}")
        return None

    ################################################
    # Normalization + DataFrame Conversion
    ################################################
    def normalize_fields(self, records: List[Dict]) -> List[Dict]:
        transformations = self.endpoint_config.get("transformations", {})
        for r in records:
            for field, func_name in transformations.items():
                if field in r and func_name in self.transformation_funcs:
                    r[field] = self.transformation_funcs[func_name](r[field])
        return records

    def to_dataframe(self, records: List[Dict]) -> pd.DataFrame:
        return pd.DataFrame(records)

    ################################################
    # Full Transformation Workflow
    ################################################
    def transform_latest_file(
        self,
        endpoint: str,
        include_geometry: bool = False,
        output_format: str = "csv"
    ) -> str:
        """
        Fully config-driven transformation of the latest raw file.
        Applies all transformations defined in endpoint_config without hardcoded checks.
        """
        try:
            raw_records = self.data_manager.load_latest_file(endpoint, use_processed=False)
            extracted = self.extract_properties(raw_records, include_geometry=include_geometry)
            normalized = self.normalize_fields(extracted)

            df = self.to_dataframe(normalized)
            output_path = self.data_manager.save_dataframe(
                df,
                endpoint,
                use_processed=True,
                output_format=output_format,
            )
            self.logger.info(f"Transformed data saved to {output_path}")
            return str(output_path)
        except Exception as e:
            msg = f"Failed to transform latest file for endpoint '{endpoint}': {e}"
            self.logger.error(msg)
            raise TransformError(msg)
