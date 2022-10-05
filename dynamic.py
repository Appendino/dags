from email.policy import default
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

patners = {
    "snowflakes": {
        "schedule": "@daily",
        "path": "/data/snowflake"
    },
    'netflix': {
        'schedule': '@weekly',
        'path': '/data/netflix'
    }, 
}

def generate_dag (dag_id, schedule_interval, details, default_args):
    with DAG(dag_id, schedule_interval=schedule_interval, default_args=default_args) as dag:
        @task.python
        def process(path):
            print(f'Processing: {path}')
        process(details['path'])
    return dag

for patner, details in patners.items():
    dag_id = f'dag_{patner}'
    default_args = {
        'start_date': datetime(2022, 10, 1)
    }
    globals()[dag_id] = generate_dag(dag_id, details['schedule'], details, default_args)