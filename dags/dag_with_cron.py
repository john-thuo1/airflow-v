from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner" : "jthuo",
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5)   
}


# Run DAG Weekly @ 03:00 from Tuesday through Friday
with DAG(dag_id="dag_cron_v3", default_args=default_args, 
         start_date = datetime(2023, 12, 9), schedule_interval="0 3 * * Tue-Fri",
    )as dag:
    
    task1 = BashOperator(
        task_id = "task1",
        bash_command = "echo dag with cron expression"
    )
    
    task1
