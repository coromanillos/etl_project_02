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
from typing import Dict
from src.logging_config import configure_logger


class USGSExporter:
    def __init__(self, config: Dict, logger=None):
        """
        Initialize the exporter with full config dict.
        """
        # Database engine
        db_config = config.get("db", {})
        db_url = self._make_db_url(db_config)
        self.engine = create_engine(db_url)

        # Export directory
        self.export_dir = Path(config["paths"]["exports"])
        self.export_dir.mkdir(parents=True, exist_ok=True)

        # Export config
        self.export_config = config.get("exports", {})

        # Logger
        self.logger = logger or configure_logger(config.get("logging"))

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

                results[export_name] = out_path
                self.logger.info(f"Export {export_name} → {out_path}")

            except Exception as e:
                self.logger.error(f"Export {export_name} failed: {e}")
                raise
        return results

    def _export_gis(self, view_name: str) -> Path:
        """
        Export PostGIS view to GeoPackage.
        """
        gdf = gpd.read_postgis(f"SELECT * FROM {view_name}", self.engine, geom_col="geometry")
        out_path = self._timestamped_path(view_name, ".gpkg")
        gdf.to_file(out_path, driver="GPKG")
        return out_path

    def _export_business(self, view_name: str) -> Path:
        """
        Export PostGIS view to CSV.
        """
        df = pd.read_sql(f"SELECT * FROM {view_name}", self.engine)
        out_path = self._timestamped_path(view_name, ".csv")
        df.to_csv(out_path, index=False)
        return out_path
