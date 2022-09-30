from datetime import datetime, timedelta
from pydoc import describe
from airflow.decorators import task, dag

@task.python
def extract():
    partner_name = "buceta"
    partner_path = "/partners/buceta"
    return partner_name

@task.python
def process(partner_name):
    print("merda")

@dag(description="Dag mothgerfucker", start_date=datetime(2022, 1, 1), schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10), tags=["buceta", "linda"], catchup=False, max_active_runs=1)
def my_dag():
    extract() >> process()