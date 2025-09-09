##################################################################################
# Name: usgs_loader.py
# Author: Christopher O. Romanillos
# Description: Config-driven loader class for PostGIS (extensible to other backends)
# Date: 09/08/25
##################################################################################

from typing import Dict, Any
import pandas as pd
import psycopg2
import psycopg2.extras

from src.exceptions import LoadError
from src.file_utils import DataManager


class BaseLoader:
    """Abstract base class for all loaders (PostGIS, S3, Azure, etc.)."""

    def __init__(self, data_manager: DataManager, logger, db_config: Dict[str, Any]):
        self.data_manager = data_manager
        self.logger = logger
        self.db_config = db_config

    def connect(self):
        """Establish connection. Must be implemented by subclasses."""
        raise NotImplementedError

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """Load data into target system. Must be implemented by subclasses."""
        raise NotImplementedError

    def close(self):
        """Close connection (default is no-op)."""
        pass


class PostGISLoader(BaseLoader):
    """Loader for PostgreSQL/PostGIS (Docker-hosted service)."""

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                dbname=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
            )
            self.conn.autocommit = False
            self.logger.info("Connected to PostGIS database successfully")
        except Exception as e:
            msg = f"Failed to connect to PostGIS: {e}"
            self.logger.error(msg)
            raise LoadError(msg)

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        if df.empty:
            self.logger.warning(f"No data to load into {table_name}")
            return

        try:
            with self.conn.cursor() as cur:
                # Dynamically build INSERT statement
                columns = list(df.columns)
                insert_sql = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT (id) DO NOTHING;
                """

                # Convert DataFrame to list of tuples
                values = [tuple(x) for x in df.to_numpy()]

                psycopg2.extras.execute_values(
                    cur, insert_sql, values, template=None, page_size=1000
                )

            self.conn.commit()
            self.logger.info(f"Loaded {len(df)} rows into {table_name}")

        except Exception as e:
            self.conn.rollback()
            msg = f"Failed to load data into {table_name}: {e}"
            self.logger.error(msg)
            raise LoadError(msg)

    def close(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
            self.logger.info("Closed PostGIS connection")


class USGSLoaderPipeline:
    """
    High-level loader pipeline for USGS endpoints.
    Compatible with CI/CD & Airflow orchestration.
    """

    def __init__(self, data_manager: DataManager, logger, config: Dict):
        self.data_manager = data_manager
        self.logger = logger
        self.config = config
        self.db_config = config.get("db", {})

    def load_latest_file(self, endpoint: str, table_name: str) -> str:
        try:
            # Load transformed data into DataFrame
            df = self.data_manager.load_latest_file(
                endpoint, use_processed=True, as_dataframe=True
            )
            self.logger.info(f"Loaded transformed DataFrame for {endpoint}: {len(df)} records")

            # Initialize loader
            loader = PostGISLoader(self.data_manager, self.logger, self.db_config)
            loader.connect()
            loader.load_dataframe(df, table_name)
            loader.close()

            return f"Data loaded successfully into {table_name} for endpoint '{endpoint}'"

        except Exception as e:
            msg = f"Load failed for endpoint '{endpoint}' into table '{table_name}': {e}"
            self.logger.error(msg)
            raise LoadError(msg)
