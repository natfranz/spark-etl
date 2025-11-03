from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'simple_test_dag',
    description='Simple test to verify Airflow is working',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test']
)

task_1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello from simple test"',
    dag=dag
)
