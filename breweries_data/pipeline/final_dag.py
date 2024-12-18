from __future__ import annotations

import datetime

import airflow
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with airflow.DAG(
    dag_id="final_dag",
    start_date=datetime.datetime(2024, 1, 1),
    schedule=None,  
    catchup=False,
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_dag",
        trigger_dag_id="dados_cervejarias__pipeline__bronze_dag",  
        wait_for_completion=True, 
        execution_date="{{ execution_date }}", 
        trigger_run_id="{{ dag.dag_id }}_{{ ts }}",
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="dados_cervejarias__pipeline__silver_dag",
        wait_for_completion=True,
        execution_date="{{ execution_date }}",
        trigger_run_id="{{ dag.dag_id }}_{{ ts }}",
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="dados_cervejarias__pipeline__gold_dag",
        wait_for_completion=True,
        execution_date="{{ execution_date }}",
        trigger_run_id="{{ dag.dag_id }}_{{ ts }}",
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> trigger_bronze >> trigger_silver >> trigger_gold >> fim
