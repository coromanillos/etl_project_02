##################################################################################
# Name: usgs_archiver.py
# Author: Christopher O. Romanillos
# Description: Archives transformed USGS files into timestamped directories
#   Exists as a substiution for cloud integration. 
# Date: 09/12/25
##################################################################################

import shutil
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Optional

from src.exceptions import ArchiverError

class USGSArchiver:
    REQUIRED_KEYS = {"data_manager", "logger", "config"}

    def __init__(self, **kwargs: Any):
        # Validate required keys
        missing = self.REQUIRED_KEYS - kwargs.keys()
        if missing:
            raise ValueError(f"Missing required arguments for USGSArchiver: {missing}")

        self.data_manager = kwargs["data_manager"]
        self.logger = kwargs["logger"]
        self.config = kwargs["config"]

        # Resolve archive directory
        try:
            self.archive_dir = Path(self.config["paths"]["archived_data"])
        except KeyError as e:
            raise ArchiverError(f"Invalid config for USGSArchiver: missing {e}")

        # Allow extensibility with extra kwargs
        self.__dict__.update(kwargs)

    def _get_timestamped_path(self, endpoint: str) -> Path:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        path = self.archive_dir / endpoint / ts
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _copy_file(self, src: Path, dest_dir: Path) -> Path:
        dest_file = dest_dir / Path(src).name
        shutil.copy(src, dest_file)
        return dest_file

    def archive_latest_file(self, endpoint: str) -> Optional[str]:
        """Archives the latest processed file for a given endpoint."""
        try:
            latest_file = self.data_manager.get_latest_file(endpoint, use_processed=True)
            if not latest_file:
                self.logger.warning(f"No file to archive for endpoint={endpoint}")
                return None

            archive_path = self._get_timestamped_path(endpoint)
            archived_file = self._copy_file(latest_file, archive_path)

            self.logger.info(f"Archived {latest_file} → {archived_file}")
            return str(archived_file)

        except Exception as e:
            msg = f"Archiving failed for endpoint={endpoint}: {e}"
            self.logger.error(msg)
            raise ArchiverError(msg)
