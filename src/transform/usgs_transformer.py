##################################################################################
# Name: usgs_transformer.py
# Author: Christopher O. Romanillos
# Description: Class-based transformation of USGS API raw data (generic)
# Date: 08/30/25
##################################################################################

from typing import List, Dict
import pandas as pd
from src.exceptions import TransformError
from src.file_utils import DataManager

class USGSTransformer:
    def __init__(self, data_manager: DataManager, logger, endpoint_config: Dict):
        self.data_manager = data_manager
        self.logger = logger
        self.endpoint_config = endpoint_config

        # Map of built-in transformation functions
        self.transformation_funcs = {
            "yesno_to_bool": self.yesno_to_bool
            # future transformations can be added here
        }

    def extract_properties(self, records: List[Dict], include_geometry: bool = False) -> List[Dict]:
        extracted = []
        for feature in records:
            props = feature.get("properties", {}) or {}
            if "id" in feature:
                props["id"] = feature["id"]
            if include_geometry and feature.get("geometry") is not None:
                props["geometry"] = feature["geometry"]
            extracted.append(props)
        self.logger.info(f"Extracted {len(extracted)} property records (with id{' and geometry' if include_geometry else ''})")
        return extracted

    def yesno_to_bool(self, value):
        if value is None:
            return None
        v = str(value).strip().upper()
        if v == "Y":
            return True
        if v == "N":
            return False
        return None

    def normalize_fields(self, records: List[Dict]) -> List[Dict]:
        """
        Apply endpoint-specific transformations as defined in config
        """
        transformations = self.endpoint_config.get("transformations", {})
        for r in records:
            for field, func_name in transformations.items():
                if field in r and func_name in self.transformation_funcs:
                    r[field] = self.transformation_funcs[func_name](r[field])
        return records

    def to_dataframe(self, records: List[Dict]) -> pd.DataFrame:
        return pd.DataFrame(records)

    def transform_latest_file(
        self,
        endpoint: str,
        include_geometry: bool = False,
        output_format: str = "csv"
    ) -> str:
        try:
            raw_records = self.data_manager.load_latest_file(endpoint, use_processed=False)
            extracted = self.extract_properties(raw_records, include_geometry=include_geometry)
            normalized = self.normalize_fields(extracted)
            df = self.to_dataframe(normalized)
            output_path = self.data_manager.save_dataframe(df, endpoint, use_processed=True, output_format=output_format)
            self.logger.info(f"Transformed data saved to {output_path}")
            return str(output_path)
        except Exception as e:
            msg = f"Failed to transform latest file for endpoint '{endpoint}': {e}"
            self.logger.error(msg)
            raise TransformError(msg)
