##################################################################################
# Name: usgs_exporter.py
# Author: Christopher O. Romanillos
# Description: Fully config-driven export of curated PostGIS views into GIS (GeoPackage) 
#              and Business Analyst (CSV) friendly formats.
# Date: 09/13/25
##################################################################################

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from src.logging_config import configure_logger


class USGSExporter:
    REQUIRED_KEYS = {"config", "logger"}

    def __init__(self, **kwargs: Any):
        """
        Initialize the exporter with full config dict.
        Expected kwargs:
          - config (dict): Full project configuration dictionary
          - logger: Logger instance
        """
        # Validate required keys
        missing = self.REQUIRED_KEYS - kwargs.keys()
        if missing:
            raise ValueError(f"Missing required arguments for USGSExporter: {missing}")

        self.config: Dict = kwargs["config"]
        self.logger = kwargs["logger"]

        # Database engine
        db_config = self.config.get("db", {})
        db_url = self._make_db_url(db_config)
        self.engine = create_engine(db_url)

        # Export directory
        self.export_dir = Path(self.config["paths"]["exports"])
        self.export_dir.mkdir(parents=True, exist_ok=True)

        # Export config
        self.export_config = self.config.get("exports", {})

        # Allow extensibility with extra kwargs
        self.__dict__.update(kwargs)

    def _make_db_url(self, db_config: Dict) -> str:
        """
        Build SQLAlchemy DB URL from config.
        """
        user = db_config.get("user", "monitoring_user")
        password = db_config.get("password", "password")
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        database = db_config.get("database", "monitoring")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def _timestamped_path(self, name: str, suffix: str) -> Path:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        return self.export_dir / f"{name}_{ts}{suffix}"

    def export_all(self) -> Dict[str, Path]:
        """
        Run all exports defined in config.yaml.
        Returns dict of export_name -> output_path.
        """
        results = {}
        for export_name, cfg in self.export_config.items():
            view_name = cfg["view_name"]
            fmt = cfg["format"]
            try:
                if fmt == "gpkg":
                    out_path = self._export_gis(view_name)
                elif fmt == "csv":
                    out_path = self._export_business(view_name)
                else:
                    raise ValueError(f"Unsupported export format: {fmt}")

                results[expor]()
