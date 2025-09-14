################################################################################
# Name: usgs_etl_dag.py
# Author: Christopher O. Romanillos
# Description: Airflow DAG to run USGS ETL, config-driven SQL view creation,
#              and exports
# Date: 09/13/25
################################################################################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2

from src.pipelines.usgs_etl_runner import run_usgs_etl
from src.pipelines.usgs_export_runner import run_usgs_exporter
from src.utils.config_loader import load_config  
from src.utils.logging_utils import setup_logger  

# -----------------------------
# DAG default arguments
# -----------------------------
default_args = {
    "owner": "monthly_data_pipeline",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------------
# Load config & logger once
# -----------------------------
config = load_config("config/config.yaml")
db_config = config["db"]
logger = setup_logger(config["logging"])

# -----------------------------
# PostGIS health check
# -----------------------------
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

# -----------------------------
# ETL wrapper functions
# -----------------------------
def run_monitoring_locations():
    return run_usgs_etl("monitoring_locations", config, db_config, logger)

def run_parameter_codes():
    return run_usgs_etl("parameter_codes", config, db_config, logger)

def run_daily_values():
    return run_usgs_etl("daily_values", config, db_config, logger)

# -----------------------------
# USGS Export wrapper function for DAG
# -----------------------------
def run_exports():
    return run_usgs_exporter(config=config, logger=logger)

# -----------------------------
# Instantiate DAG
# -----------------------------
with DAG(
    "usgs_etl_dag",
    default_args=default_args,
    description="ETL for USGS endpoints, SQL view creation, and exports",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 11),
    catchup=False,
    tags=["usgs", "etl"],
) as dag:

    # PostGIS health check
    postgis_health = PythonOperator(
        task_id="check_postgis",
        python_callable=check_postgis,
    )

    # ETL tasks
    t_monitoring_locations = PythonOperator(
        task_id="etl_monitoring_locations",
        python_callable=run_monitoring_locations,
    )

    t_parameter_codes = PythonOperator(
        task_id="etl_parameter_codes",
        python_callable=run_parameter_codes,
    )

    t_daily_values = PythonOperator(
        task_id="etl_daily_values",
        python_callable=run_daily_values,
    )

    # Execute SQL view creation tasks
    view_tasks = []
    for view_name, cfg in config.get("views", {}).items():
        task = PostgresOperator(
            task_id=f"create_{view_name}",
            postgres_conn_id="postgis_default",
            sql=cfg["sql_file"],
            autocommit=True,
        )
        view_tasks.append(task)

    # Export task (runs all config-driven exports)
    t_usgs_exporter = PythonOperator(
        task_id="usgs_exporter",
        python_callable=run_exports,
    )

    # -----------------------------
    # DAG dependencies
    # -----------------------------
    postgis_health >> [t_monitoring_locations, t_parameter_codes]
    [t_monitoring_locations, t_parameter_codes] >> t_daily_values
    t_daily_values >> view_tasks
    view_tasks >> t_usgs_exporter
