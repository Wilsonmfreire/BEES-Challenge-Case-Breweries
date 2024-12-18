from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="silver_layer",
    schedule=None, 
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["silver", "transformation"],
) as dag:
    extract_from_api_to_json = PapermillOperator(
        task_id="transform_data",
        input_nb=os.path.join(DAG_FOLDER, "..", "data_injection", "notebook", "silver_layer.ipynb")
        output_nb="/tmp/silver_output.ipynb",
        parameters={
            "config_path": os.path.join(DAG_FOLDER, "..", "data_injection", "config.json"),
        },
    )