##################################################################################
# Name: usgs_transformer.py
# Author: Christopher O. Romanillos
# Description: Class-based transformation of USGS API raw data (generic)
# Date: 08/30/25
##################################################################################

import json
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd


class USGSTransformer:
    def __init__(self, raw_data_dir: str, transformed_data_dir: str, logger):
        """
        Transformer for USGS raw JSON files.

        :param raw_data_dir: Path to directory containing raw JSON files
        :param transformed_data_dir: Path to save transformed outputs
        :param logger: Logger instance
        """
        self.raw_data_dir = Path(raw_data_dir)
        self.transformed_data_dir = Path(transformed_data_dir)
        self.logger = logger

        self.transformed_data_dir.mkdir(parents=True, exist_ok=True)

    ##############################################################
    # Load raw JSON file and return just the "properties" records
    ##############################################################
    def extract_properties(self, raw_file: Path, include_geometry: bool = False) -> List[Dict]:
        """
        Extract only the properties from a raw USGS GeoJSON file.

        :param raw_file: Path to a raw JSON file
        :param include_geometry: If True, also flatten geometry coordinates into lat/lon
        :return: List of property dictionaries
        """
        with open(raw_file, "r") as f:
            data = json.load(f)

        features = data.get("features", [])
        records = []

        for feature in features:
            props = feature.get("properties", {}) or {}

            if include_geometry and feature.get("geometry") and feature["geometry"].get("coordinates"):
                coords = feature["geometry"]["coordinates"]
                props["longitude"], props["latitude"] = coords[0], coords[1]

            records.append(props)

        self.logger.info(f"Extracted {len(records)} records from {raw_file.name}")
        return records

    ##############################################################
    # Convert properties into DataFrame
    ##############################################################
    def to_dataframe(self, records: List[Dict]) -> pd.DataFrame:
        """Convert list of property dicts into a Pandas DataFrame."""
        return pd.DataFrame(records)

    ##############################################################
    # Full transform pipeline: raw JSON -> cleaned CSV
    ##############################################################
    def transform_file(self, raw_file: Path, include_geometry: bool = False, output_format: str = "csv") -> Path:
        """
        Transform a raw JSON file into a structured CSV or Parquet file.

        :param raw_file: Path to raw JSON file
        :param include_geometry: Whether to include lat/lon from geometry
        :param output_format: "csv" or "parquet"
        :return: Path to transformed file
        """
        records = self.extract_properties(raw_file, include_geometry=include_geometry)
        df = self.to_dataframe(records)

        output_file = self.transformed_data_dir / f"{raw_file.stem}.{output_format}"
        if output_format == "csv":
            df.to_csv(output_file, index=False)
        elif output_format == "parquet":
            df.to_parquet(output_file, index=False)
        else:
            raise ValueError(f"Unsupported format: {output_format}")

        self.logger.info(f"Saved transformed file to {output_file}")
        return output_file
