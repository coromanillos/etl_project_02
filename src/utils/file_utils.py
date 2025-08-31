from pathlib import Path
import json
from typing import List, Dict
from datetime import datetime

from src.exceptions import SaveError, TransformError


class DataManager:
    def __init__(self, logger, base_raw_dir: Path, base_processed_dir: Path, timestamp_format: str = "%Y-%m-%dT%H:%M:%S"):
        """
        Handles saving/loading of raw and processed JSON data with timestamped filenames.
        """
        self.logger = logger
        self.base_raw_dir = base_raw_dir
        self.base_processed_dir = base_processed_dir
        self.timestamp_format = timestamp_format

    def _get_directory(self, endpoint: str, use_processed: bool = False) -> Path:
        """Return endpoint-specific directory, creating it if needed."""
        base_dir = self.base_processed_dir if use_processed else self.base_raw_dir
        path = base_dir / endpoint
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _make_timestamped_path(self, directory: Path, endpoint: str) -> Path:
        """Generate a timestamped filename in the directory."""
        timestamp = datetime.now().strftime(self.timestamp_format)
        return directory / f"{endpoint}_{timestamp}.json"

    def save_file(
        self,
        data: List[Dict],
        endpoint: str,
        use_processed: bool = False
    ) -> Path:
        """Save data (list of dicts) to raw or processed directory as JSON."""
        if not data:
            self.logger.warning(f"No data to save for endpoint '{endpoint}' (list empty)")
            return None

        try:
            directory = self._get_directory(endpoint, use_processed)
            file_path = self._make_timestamped_path(directory, endpoint)

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"Saved data to {file_path}")
            return file_path

        except Exception as e:
            msg = f"Failed to save JSON file for endpoint '{endpoint}': {e}"
            self.logger.error(msg)
            raise SaveError(msg)

    def load_latest_file(
        self,
        endpoint: str,
        use_processed: bool = False
    ) -> List[Dict]:
        """Load the most recent JSON file for the endpoint (raw or processed)."""
        directory = self._get_directory(endpoint, use_processed)
        files = list(directory.glob(f"{endpoint}_*.json"))

        if not files:
            raise FileNotFoundError(f"No files found for endpoint '{endpoint}' in {directory}")

        latest_file = max(files, key=lambda f: f.stat().st_mtime)

        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                records = json.load(f)
            self.logger.info(f"Loaded {len(records)} records from {latest_file.name}")
            return records

        except Exception as e:
            msg = f"Failed to load JSON file {latest_file}: {e}"
            self.logger.error(msg)
            raise TransformError(msg)
