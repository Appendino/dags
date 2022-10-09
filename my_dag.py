from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow import DAG

with DAG("my_dag", description="Dag da buceta", 
    start_date=datetime(2022, 10, 1), 
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["buceta_louca", "xoxota"]) as dag:

    start_task = DummyOperator("start_task")
    end_task = DummyOperator("end_task")

    start_task >> end_task