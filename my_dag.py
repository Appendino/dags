from concurrent.futures import process
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

def _extract(partner_name):
    print(partner_name)

def _process():
    print("process")

with DAG("my_dag", description="Dag da buceta", 
    start_date=datetime(2022, 10, 1), 
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["buceta_louca", "xoxota"], catchup=False) as dag:

    start_task = DummyOperator(task_id="start_task")
    end_task = DummyOperator(task_id="end_task")

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        op_args="{{ var.json.my_dag_partner.name }}"
    )

    process = PythonOperator(
        task_id="process"
        python_callable=_process
    )

    start_task >> extract >> process >> end_task