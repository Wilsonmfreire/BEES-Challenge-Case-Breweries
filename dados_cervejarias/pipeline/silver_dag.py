from __future__ import annotations

import datetime
import pendulum
import os
import sys

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(DAG_FOLDER, "..", "dados_cervejarias", "data_injestion", "script")
sys.path.append(SCRIPTS_DIR)

import transform_silver

with DAG(
    dag_id="silver_layer",
    schedule=None,  # Set a schedule if desired
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["silver", "transformation"],
) as dag:
    transform_silver_task = PythonOperator(
        task_id="transform_silver_data",
        python_callable=transform_silver.main,
    )
