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
import pandas as pd
from src.usgs_monitoring_locations.wrapper_etl import extract_task, transform_task, load_task
from utils.config import Config
from utils.db_config import get_engine
from models.monitoring_schema import metadata
from src.common.http_client import RequestsHttpClient  # example placeholder

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="usgs_monitoring_etl_v2",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["usgs", "etl", "monitoring"],
    description="ETL pipeline for USGS monitoring locations using modern refactored scripts.",
) as dag:

    def run_extract(**kwargs):
        config = Config()
        http_client = RequestsHttpClient()
        url = config.monitoring_locations_url

        df = extract_task(url, http_client)
        kwargs["ti"].xcom_push(key="df", value=df.to_dict(orient="records"))

    def run_transform(**kwargs):
        records = kwargs["ti"].xcom_pull(task_ids="extract_usgs_data", key="df")
        df = pd.DataFrame(records)
        transformed_df = transform_task(df)
        kwargs["ti"].xcom_push(key="df_transformed", value=transformed_df.to_dict(orient="records"))

    def run_load(**kwargs):
        records = kwargs["ti"].xcom_pull(task_ids="transform_usgs_data", key="df_transformed")
        df = pd.DataFrame(records)
        load_task(df, session_factory=get_engine, metadata=metadata)

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
