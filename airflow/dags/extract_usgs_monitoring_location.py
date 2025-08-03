#####################################################################
# Name: extract_usgs_monitoring_locations.py
# Author: Christopher O. Romanillos
# Description: USGS monitoring_location ETL DAG orchestration script
# Date: 08/03/25
#####################################################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.usgs_monitoring_locations.wrapper_etl import extract_task, transform_task, load_task

default_args = {
    'owner': 'airflow',
    'retries': 2,
}

with DAG(
    dag_id="usgs_monitoring_etl",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["usgs", "etl", "monitoring"],
) as dag:

    extract = PythonOperator(
        task_id="extract_usgs_data",
        python_callable=extract_task,
    )

    def wrap_transform(**kwargs):
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(task_ids="extract_usgs_data")
        return transform_task(raw_data)

    transform = PythonOperator(
        task_id="transform_usgs_data",
        python_callable=wrap_transform,
    )

    def wrap_load(**kwargs):
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids="transform_usgs_data")
        load_task(records)

    load = PythonOperator(
        task_id="load_usgs_data",
        python_callable=wrap_load,
    )

    extract >> transform >> load
