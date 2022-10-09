from asyncio import Task
from concurrent.futures import process
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.operators.subdag import SubDagOperator
from datetime import datetime, timedelta
from typing import Dict

from airflow.utils.task_group import TaskGroup


@task.python(task_id="extract_patners", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/patners/netflox"
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process_a(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_b(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_c(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

default_args = {
    "start_date": datetime(2022, 10, 1)
}
with DAG("my_dag_decor", description="Dag da buceta", 
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=10),
    tags=["buceta_louca", "xoxota"], catchup=False) as dag:

    partner_settings = extract()

    with TaskGroup(group_id="process_tasks") as process_tasks:
        process_a(partner_settings["partner_name"], partner_settings["partner_path"])
        process_b(partner_settings["partner_name"], partner_settings["partner_path"])
        process_c(partner_settings["partner_name"], partner_settings["partner_path"])
