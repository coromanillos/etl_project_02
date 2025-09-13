##################################################################################
# Name: usgs_archiver.py
# Author: Christopher O. Romanillos
# Description: Archives transformed USGS files into timestamped directories
# Date: 09/12/25
##################################################################################

import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional


class USGSArchiver:
    def __init__(self, data_manager, logger, config: dict):
        self.data_manager = data_manager
        self.logger = logger
        self.archive_dir = Path(config["paths"]["archived_data"])

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
            self.logger.error(f"Archiving failed for endpoint={endpoint}: {e}")
            return None

# Add archiver exception class