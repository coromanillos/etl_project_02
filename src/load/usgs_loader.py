##################################################################################
# Name: usgs_loader.py
# Author: Christopher O. Romanillos
# Description: Config-driven loader for USGS data into PostGIS (Docker service)
# Date: 09/08/25
##################################################################################

"""
If anyone is reading this and wondering about the approach, I think ideally I would
have gone for a base class that supports loading to both S3 and PostgreSQL. 

It makes sense to me to have these tasks share a base case just because the nature of
the tasks are so similar, but becuase I am focusing primarily on the pipeline
and have not done research into Azure/S3 free trials that I could incorporate into my 
project, I have not done so.

In hindsight, I would have applied the same line of thinking to the extraction class...
"""

import os
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

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Load a Pandas DataFrame into PostGIS using COPY-like bulk insert.
        Assumes DataFrame columns match table schema.
        """
        if df.empty:
            self.logger.warning(f"No records to load into {table_name}")
            return

        try:
            with self.conn.cursor() as cur:
                columns = list(df.columns)
                values = [tuple(x) for x in df.to_numpy()]
                insert_query = f"""
                    INSERT INTO {table_name} ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """
                execute_values(cur, insert_query, values)
            self.conn.commit()
            self.logger.info(f"Loaded {len(df)} records into {table_name}")
        except Exception as e:
            self.conn.rollback()
            raise LoadError(f"Failed to load data into {table_name}: {e}")

    def load_latest_file(self, endpoint: str, use_processed: bool = True) -> str:
        """
        Orchestrates loading of the latest transformed file for a given endpoint.
        """
        table_name = f"public.{endpoint}"
        try:
            # 1. Load latest transformed file into DataFrame
            df = self.data_manager.load_latest_file(endpoint, use_processed=use_processed, as_dataframe=True)
            self.logger.info(f"Loaded latest file for endpoint={endpoint}, rows={len(df)}")

            # 2. Connect to PostGIS
            self.connect()

            # 3. Load into PostGIS
            self.load_dataframe(df, table_name)

            return f"Successfully loaded {len(df)} records into {table_name}"

        except Exception as e:
            msg = f"Load failed for endpoint={endpoint}: {e}"
            self.logger.error(msg)
            raise LoadError(msg)
        finally:
            self.close()
