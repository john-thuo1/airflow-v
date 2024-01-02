from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'jthuo',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5),
    
}

# Note : xcom limits data transfers to 48kb

# ti is task instance and we want to get the name from another task
def greet(age, ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    year = ti.xcom_pull(task_ids="get_year", key="year")

    print(f"Hey! My Name is {first_name} {last_name}. I am {age} years old and in Year{year}!")

# For Single Value 
# def get_name():
#     return "John" 

# For multiple value names

def get_name(ti):
    ti.xcom_push(key = "first_name", value = "Jaimey")
    ti.xcom_push(key = "last_name", value = "Ant")
   
def get_year(ti):
    ti.xcom_push(key = "year", value = "One(1)") 


    
with DAG (dag_id='pythonOp_v6', default_args=default_args, description="This is our first Python Operator DAG",
    start_date = datetime(2023, 12, 31), schedule_interval='@daily') as dag: 
    
    task1 = PythonOperator(
        task_id = "Greet",
        python_callable = greet,
        op_kwargs = {"age" : 17}
    )
    
    task2 = PythonOperator(
        task_id = "get_name",
        python_callable = get_name,
        
        )
    task3 = PythonOperator(
        task_id = "get_year", 
        python_callable = get_year
    )
    
    task2.set_downstream(task1)
    task2.set_downstream(task3)