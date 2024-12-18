from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="gold_layer",
    schedule=None, 
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["gold", "aggregation"],
) as dag:
    extract_from_api_to_json = PapermillOperator(
        task_id="aggregate data",
        input_nb=os.path.join(DAG_FOLDER, "..", "data_injection", "notebook", "gold_layer.ipynb")
        output_nb="/tmp/gold_output.ipynb",
        parameters={
            "config_path": os.path.join(DAG_FOLDER, "..", "data_injection", "config.json"),
        },
    )