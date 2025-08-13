#####################################################################
# Name: usgs_monitoring_etl_streaming.py
# Author: Christopher O. Romanillos (Refactored by ChatGPT)
# Description: Streaming ETL DAG for USGS monitoring locations
# Date: 08/13/25
#####################################################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from src.usgs_monitoring_locations.wrapper_etl_streaming import run_streaming_etl
from utils.config import get_config
from src.common.http_client import RequestsHttpClient  

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="usgs_monitoring_etl_streaming",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["usgs", "etl", "monitoring"],
    description="Streaming ETL pipeline for USGS monitoring locations",
) as dag:

    def etl_task(**kwargs):
        """
        Run the streaming ETL task. 
        Handles per-page extraction and transformation.
        """
        config = get_config()
        http_client = RequestsHttpClient()
        run_streaming_etl(url=config.monitoring_locations_url, http_client=http_client)

    streaming_etl = PythonOperator(
        task_id="run_streaming_etl",
        python_callable=etl_task,
    )

    streaming_etl
