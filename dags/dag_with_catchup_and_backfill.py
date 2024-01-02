from airflow import DAG 
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner' : 'jthuo',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5),
    
}

with DAG(dag_id='dag_catchup_backfill', default_args=default_args, description="DAG with Catchup&Backfill",
    start_date = datetime(2023, 12, 31), schedule_interval='@daily', catchup=False) as dag: 
    
    task1 = BashOperator(
        task_id ="t1",
        bash_command = "echo Simple Bash Command!"
    )
            
    task1 
