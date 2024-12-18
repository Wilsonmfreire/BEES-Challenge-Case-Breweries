import datetime

from airflow.models.dag import DAG

with DAG(
    dag_id="run_brewery_process",
    schedule=datetime.timedelta(hours=4),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example3"],
    
) as dag:
    
    def execute_ingestao_bronze():
    
    task1 = EmptyOperator(task_id="ingestao_bronze")
    task2 = EmptyOperator(task_id="transform_silver")
    task3 = EmptyOperator(task_id="aggregate_gold")

    task1 >> task2 >> task3