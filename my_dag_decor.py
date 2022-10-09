from concurrent.futures import process
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.operators.subdag import SubDagOperator
from datetime import datetime, timedelta
from typing import Dict

from subdag_factory import subdag_factory

@task.python(task_id="extract_patners")
def extract() -> Dict[str, str]:
    partner_name = "netflix"
    partner_path = "/patners/netflox"
    return {"partner_name": partner_name, "partner_path": partner_path}

default_args = {
    "start_date": datetime(2022, 10, 1)
}
with DAG("my_dag_decor", description="Dag da buceta", 
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=10),
    tags=["buceta_louca", "xoxota"], catchup=False) as dag:

    process_tasks = SubDagOperator(
        task_id="process_tasks",
        subdag=subdag_factory("my_dag_decor", "process_tasks",
        default_args)
    )

    extract() >> process_tasks