from concurrent.futures import process
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

from datetime import datetime, timedelta
from typing import Dict

@task.python(task_id="extract_patners")
def extract() -> Dict[str, str]:
    partner_name = "netflix"
    partner_path = "/patners/netflox"
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process(partner_name, partner_path):
    print("========================================")
    print(partner_name)
    print(partner_path)
    print("========================================")

with DAG("my_dag_decor", description="Dag da buceta", 
    start_date=datetime(2022, 10, 1), 
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["buceta_louca", "xoxota"], catchup=False) as dag:

    partner_settings = extract()
    process(partner_settings['partner_name'], partner_settings['partner_path'])