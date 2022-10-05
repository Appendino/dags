from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import cross_downstream, chain
from datetime import datetime


default_args = {
    "start_date": datetime(2022, 10, 1)
}

with DAG("dependency_0_0_1", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    t1 = DummyOperator(task_id="t1")
    t2 = DummyOperator(task_id="t2")
    t3 = DummyOperator(task_id="t3")
    t4 = DummyOperator(task_id="t4")
    t5 = DummyOperator(task_id="t5")
    t6 = DummyOperator(task_id="t6")
    t7 = DummyOperator(task_id="t7")

    cross_downstream([t1, t2, t3], [t4, t5, t6])
    [t1, t2, t3] >> t7