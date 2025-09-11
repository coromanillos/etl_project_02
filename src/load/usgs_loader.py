##################################################################################
# Name: usgs_loader.py
# Author: Christopher O. Romanillos
# Description: Config-driven loader for USGS data into PostGIS (Docker service)
# Date: 09/08/25
##################################################################################

"""
Supports two loading modes:
- replace >> full table refresh (for static reference data, e.g., monitoring locations and parameter codes)
- upsert  >> insert new records or update existing ones (for fact tables, e.g., daily values)
"""

import psycopg2
import pandas as pd
from typing import Dict
from psycopg2.extras import execute_values

from src.exceptions import LoadError
from src.file_utils import DataManager


class USGSLoader:
    def __init__(self, data_manager: DataManager, logger, db_config: Dict, endpoint_config: Dict):
        self.data_manager = data_manager
        self.logger = logger
        self.db_config = db_config
        self.endpoint_config = endpoint_config
        self.conn = None

    def connect(self):
        """Establish a connection to PostGIS using psycopg2."""
        try:
            self.conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                dbname=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
            )
            self.logger.info("Connected to PostGIS")
        except Exception as e:
            raise LoadError(f"Failed to connect to PostGIS: {e}")

    def close(self):
        """Close the PostGIS connection."""
        if self.conn:
            self.conn.close()
            self.logger.info("Closed PostGIS connection")

    def load_dataframe(self, df: pd.DataFrame, table_name: str, mode: str = None):
        """
        Load a Pandas DataFrame into PostGIS.

        Args:
            df (pd.DataFrame): DataFrame to load
            table_name (str): Target PostGIS table
            mode (str): "replace" or "upsert" (falls back to endpoint_config)
        """
        if df.empty:
            self.logger.warning(f"No records to load into {table_name}")
            return

        # fallback to endpoint_config if mode not passed
        if mode is None:
            mode = self.endpoint_config.get("load_mode", "upsert")

        try:
            with self.conn.cursor() as cur:
                if mode == "replace":
                    # Full refresh: drop existing rows
                    cur.execute(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE')
                    self.logger.info(f"Truncated {table_name} before reloading")

                # Quote column names to avoid reserved word conflicts
                columns = list(df.columns)
                quoted_columns = ",".join([f'"{col}"' for col in columns])
                values = [tuple(x) for x in df.to_numpy()]

                if mode == "replace":
                    insert_query = f"""
                        INSERT INTO {table_name} ({quoted_columns})
                        VALUES %s
                    """
                elif mode == "upsert":
                    pk_fields = self.endpoint_config.get("primary_key", [])
                    if not pk_fields:
                        raise LoadError(f"No primary_key defined for endpoint {table_name}")

                    quoted_pks = ",".join([f'"{pk}"' for pk in pk_fields])
                    update_assignments = ",".join(
                        [f'"{col}"=EXCLUDED."{col}"' for col in columns if col not in pk_fields]
                    )

                    insert_query = f"""
                        INSERT INTO {table_name} ({quoted_columns})
                        VALUES %s
                        ON CONFLICT ({quoted_pks})
                        DO UPDATE SET {update_assignments}
                    """
                else:
                    raise LoadError(f"Unsupported load mode: {mode}")

                execute_values(cur, insert_query, values)

            self.conn.commit()
            self.logger.info(f"{mode.capitalize()} loaded {len(df)} records into {table_name}")

        except Exception as e:
            self.conn.rollback()
            raise LoadError(f"Failed to load data into {table_name}: {e}")

    def load_latest_file(self, endpoint: str, use_processed: bool = True) -> str:
        """
        Orchestrates loading of the latest transformed file for a given endpoint.
        """
        table_name = f"public.{endpoint}"
        mode = self.endpoint_config.get("load_mode", "upsert")
        try:
            # 1. Load latest transformed file into DataFrame
            df = self.data_manager.load_latest_file(endpoint, use_processed=use_processed, as_dataframe=True)
            self.logger.info(f"Loaded latest file for endpoint={endpoint}, rows={len(df)}")

            # 2. Connect to PostGIS
            self.connect()

            # 3. Load into PostGIS with mode
            self.load_dataframe(df, table_name, mode=mode)

            return f"Successfully {mode}-loaded {len(df)} records into {table_name}"

        except Exception as e:
            msg = f"Load failed for endpoint={endpoint}: {e}"
            self.logger.error(msg)
            raise LoadError(msg)
        finally:
            self.close()
