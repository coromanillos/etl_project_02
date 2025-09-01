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
    def __init__(self, data_manager: DataManager, logger):
        """
        Generic Transformer for any USGS data endpoint.
        """
        self.data_manager = data_manager
        self.logger = logger

    def extract_properties(self, records: List[Dict], include_geometry: bool = False) -> List[Dict]:
        """
        Extract and flatten fields from USGS API records:
          - Flattens `properties` into a dict
          - Adds top-level `id` field
          - Optionally includes raw `geometry`
        """
        extracted = []
        for feature in records:
            props = feature.get("properties", {}) or {}

            # Always include top-level id if present
            if "id" in feature:
                props["id"] = feature["id"]

            # Optionally include raw geometry
            if include_geometry and feature.get("geometry") is not None:
                props["geometry"] = feature["geometry"]

            extracted.append(props)

        self.logger.info(f"Extracted {len(extracted)} property records (with id{' and geometry' if include_geometry else ''})")
        return extracted

    def to_dataframe(self, records: List[Dict]) -> pd.DataFrame:
        """Convert list of flattened dicts into a Pandas DataFrame."""
        return pd.DataFrame(records)

    def transform_latest_file(
        self,
        endpoint: str,
        include_geometry: bool = False,
        output_format: str = "csv"
    ) -> str:
        """
        Load the latest raw file, transform, and save using DataManager.
        """
        try:
            raw_records = self.data_manager.load_latest_file(endpoint, use_processed=False)
            transformed_records = self.extract_properties(raw_records, include_geometry=include_geometry)
            df = self.to_dataframe(transformed_records)
            output_path = self.data_manager.save_dataframe(df, endpoint, use_processed=True, output_format=output_format)
            self.logger.info(f"Transformed data saved to {output_path}")
            return str(output_path)
        except Exception as e:
            msg = f"Failed to transform latest file for endpoint '{endpoint}': {e}"
            self.logger.error(msg)
            raise TransformError(msg)
