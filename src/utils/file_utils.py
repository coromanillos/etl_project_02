from pathlib import Path
import pandas as pd
import json
from typing import Union, List, Dict, Literal
from datetime import datetime

from src.exceptions import SaveError, TransformError


class DataManager:
    def __init__(self, logger, base_raw_dir: Path, base_processed_dir: Path, timestamp_format: str = "%Y-%m-%dT%H:%M:%S"):
        """
        DataManager orchestrates file handling for ETL pipelines.
        It abstracts saving and loading of raw and processed data, ensuring consistent
        directory structure, timestamped filenames, and proper data types.

        :param logger: Logging instance for reporting operations and warnings
        :param base_raw_dir: Base directory for raw (extracted) data
        :param base_processed_dir: Base directory for processed (transformed) data
        :param timestamp_format: Format used for timestamped filenames
        """
        self.logger = logger
        self.base_raw_dir = base_raw_dir
        self.base_processed_dir = base_processed_dir
        self.timestamp_format = timestamp_format

    def _get_directory(self, endpoint: str, use_processed: bool = False) -> Path:
        """
        Determine the correct directory for a given endpoint.
        Ensures the directory exists before saving or loading files.
        
        Role in pipeline:
        - Keeps raw and processed data organized per endpoint
        - Automatically creates directories if they don't exist
        - Supports both raw_data and processed_data workflows

        :param endpoint: Name of the API endpoint or data source
        :param use_processed: If True, returns processed data directory; otherwise, raw
        :return: Path object pointing to the endpoint-specific directory
        """
        base_dir = self.base_processed_dir if use_processed else self.base_raw_dir
        path = base_dir / endpoint
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _make_timestamped_path(self, directory: Path, endpoint: str, file_type: str) -> Path:
        """
        Generate a full file path with timestamp for consistent versioning.
        
        Role in pipeline:
        - Ensures unique filenames for each ETL run
        - Facilitates historical tracking and reproducibility
        - Supports multiple formats (JSON, Parquet) for different stages

        :param directory: Target directory for the file
        :param endpoint: Name of the endpoint to prefix filename
        :param file_type: File extension ('json' or 'parquet')
        :return: Full Path object for the timestamped file
        """
        timestamp = datetime.now().strftime(self.timestamp_format)
        return directory / f"{endpoint}_{timestamp}.{file_type}"

    def save_file(
        self,
        data: Union[pd.DataFrame, List[Dict]],
        endpoint: str,
        file_type: Literal["parquet", "json"],
        use_processed: bool = False
    ) -> Path:
        """
        Save a dataset (raw or processed) to the appropriate directory with timestamp.
        
        Role in pipeline:
        - Stores extracted or transformed data in a structured location
        - Supports both JSON and Parquet for flexibility in storage and downstream processing
        - Logs the number of records saved and any warnings for empty datasets
        - Raises SaveError if saving fails, ensuring pipeline fails fast

        :param data: DataFrame (for Parquet) or list of dicts (for JSON)
        :param endpoint: Name of the API endpoint or data source
        :param file_type: File format ('json' or 'parquet')
        :param use_processed: If True, saves to processed_data; otherwise, raw_data
        :return: Path of the saved file
        """
        if isinstance(data, pd.DataFrame) and data.empty:
            self.logger.warning(f"No data to save for endpoint '{endpoint}' (DataFrame empty)")
            return None
        if isinstance(data, list) and not data:
            self.logger.warning(f"No data to save for endpoint '{endpoint}' (list empty)")
            return None

        try:
            directory = self._get_directory(endpoint, use_processed)
            file_path = self._make_timestamped_path(directory, endpoint, file_type)

            if file_type == "parquet":
                if not isinstance(data, pd.DataFrame):
                    raise TypeError("Parquet saving requires a pandas DataFrame")
                data.to_parquet(file_path, index=False)

            elif file_type == "json":
                if not isinstance(data, list):
                    raise TypeError("JSON saving requires a list of dictionaries")
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"Saved data to {file_path}")
            return file_path

        except Exception as e:
            msg = f"Failed to save {file_type.upper()} file for endpoint '{endpoint}': {e}"
            self.logger.error(msg)
            raise SaveError(msg)

    def load_latest_file(
        self,
        endpoint: str,
        file_type: Literal["parquet", "json"],
        use_processed: bool = False
    ) -> Union[pd.DataFrame, List[Dict]]:
        """
        Load the most recently saved file for a given endpoint and file type.
        
        Role in pipeline:
        - Provides a simple way to access the latest ETL output
        - Supports both raw and processed data retrieval
        - Raises TransformError if reading fails, ensuring pipeline reliability
        - Facilitates incremental processing or validation steps in the ETL

        :param endpoint: Name of the API endpoint or data source
        :param file_type: File format ('json' or 'parquet')
        :param use_processed: If True, loads from processed_data; otherwise, raw_data
        :return: Loaded data as a DataFrame (for Parquet) or list of dicts (for JSON)
        """
        directory = self._get_directory(endpoint, use_processed)
        files = list(directory.glob(f"{endpoint}_*.{file_type}"))

        if not files:
            raise FileNotFoundError(f"No files found for endpoint '{endpoint}' in {directory}")

        latest_file = max(files, key=lambda f: f.stat().st_mtime)

        try:
            if file_type == "parquet":
                df = pd.read_parquet(latest_file)
                self.logger.info(f"Loaded {len(df)} records from {latest_file.name}")
                return df

            elif file_type == "json":
                with open(latest_file, "r", encoding="utf-8") as f:
                    records = json.load(f)
                self.logger.info(f"Loaded {len(records)} records from {latest_file.name}")
                return records

        except Exception as e:
            msg = f"Failed to load {file_type.upper()} file {latest_file}: {e}"
            self.logger.error(msg)
            raise TransformError(msg)
