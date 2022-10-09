from concurrent.futures import process
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

from datetime import datetime, timedelta

@task.python
def extract():
    partner_name = "netflix"
    partner_path = "/patners/netflox"
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def _process():
    print("process")

with DAG("my_dag", description="Dag da buceta", 
    start_date=datetime(2022, 10, 1), 
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["buceta_louca", "xoxota"], catchup=False) as dag:

    extract() >> process()