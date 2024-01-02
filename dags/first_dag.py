from airflow import DAG 
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner' : 'jthuo',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2),
    
}

with DAG(dag_id='first_1', default_args=default_args, description="This is our first DAG",
    start_date = datetime(2023, 12, 31), schedule_interval='@daily') as dag: 
    
    task1 = BashOperator(
        task_id ="first_task",
        bash_command = "echo hello world, this is the first task."
    )
    
    task2 = BashOperator(
        task_id = "second_task",
        bash_command = "echo Second Task."
    )
    
    task3 = BashOperator(
        task_id = "third_task",
        bash_command = "echo Third Task."
    )
    
    task1 >> task2
    task1 >> task3