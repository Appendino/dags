from datetime import datetime, timedelta
from airflow import DAG

with DAG("my_dag", description="my first dag",
    start_date=datetime(2021, 1, 1), schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10), tags=["data_science", "buceta"],
    catchup=False) as dag:
    None
