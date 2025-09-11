################################################################################
# Name: usgs_etl_dag.py
# Author: Christopher O. Romanillos
# Description: Airflow DAG to run USGS ETL for multiple endpoints with PostGIS health check
# Date: 09/11/25
################################################################################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.usgs_etl_runner import run_usgs_etl
from src.utils.config_loader import load_config  # config loader function
from src.utils.logging_utils import setup_logger  # logger setup function
import psycopg2

# DAG default arguments
default_args = {
    "owner": "christopher",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Load config and logger once
config = load_config("config/config.yaml")
db_config = config["db"]
logger = setup_logger(config["logging"])

# Define PostGIS health check
def check_postgis():
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            connect_timeout=5
        )
        conn.close()
        logger.info("PostGIS is healthy")
    except Exception as e:
        logger.error(f"PostGIS health check failed: {e}")
        raise

# Wrapper functions for ETL tasks
def run_monitoring_locations():
    return run_usgs_etl("monitoring_locations", config, db_config, logger)

def run_daily_values():
    return run_usgs_etl("daily_values", config, db_config, logger)

def run_parameter_codes():
    return run_usgs_etl("parameter_codes", config, db_config, logger)

# Instantiate DAG
with DAG(
    "usgs_etl_dag",
    default_args=default_args,
    description="ETL for USGS monitoring locations, daily values, and parameter codes with PostGIS health check",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 11),
    catchup=False,
    tags=["usgs", "etl"],
) as dag:

    # 1️⃣ PostGIS health check task
    postgis_health = PythonOperator(
        task_id="check_postgis",
        python_callable=check_postgis,
    )

    # 2️⃣ ETL tasks
    t_monitoring_locations = PythonOperator(
        task_id="etl_monitoring_locations",
        python_callable=run_monitoring_locations,
    )

    t_daily_values = PythonOperator(
        task_id="etl_daily_values",
        python_callable=run_daily_values,
    )

    t_parameter_codes = PythonOperator(
        task_id="etl_parameter_codes",
        python_callable=run_parameter_codes,
    )

    # Set dependencies: ETL tasks run after PostGIS is healthy
    postgis_health >> [t_monitoring_locations, t_daily_values, t_parameter_codes]
