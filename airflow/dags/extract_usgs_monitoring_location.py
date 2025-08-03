#####################################################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: USGS monitoring_location ETL DAG orchestration script
# Refactored: 08/03/25
#####################################################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from src.usgs_monitoring_locations.wrapper_etl import extract_task, transform_task, load_task
from airflow.models.xcom_arg import XComArg

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_extract(**kwargs) -> dict:
    """
    Extract raw monitoring location data.
    """
    raw_data = extract_task()
    kwargs["ti"].xcom_push(key="raw_data", value=raw_data)

def run_transform(**kwargs) -> list:
    """
    Transform raw extracted data into cleaned records.
    """
    raw_data = kwargs["ti"].xcom_pull(task_ids="extract_usgs_data", key="raw_data")
    transformed = transform_task(raw_data)
    kwargs["ti"].xcom_push(key="records", value=transformed)

def run_load(**kwargs) -> None:
    """
    Load cleaned records into PostgreSQL.
    """
    records = kwargs["ti"].xcom_pull(task_ids="transform_usgs_data", key="records")
    load_task(records)

with DAG(
    dag_id="usgs_monitoring_etl_v1",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["usgs", "etl", "monitoring"],
    description="ETL pipeline for USGS monitoring locations using custom wrapper scripts.",
) as dag:

    extract = PythonOperator(
        task_id="extract_usgs_data",
        python_callable=run_extract,
    )

    transform = PythonOperator(
        task_id="transform_usgs_data",
        python_callable=run_transform,
    )

    load = PythonOperator(
        task_id="load_usgs_data",
        python_callable=run_load,
    )

    extract >> transform >> load
