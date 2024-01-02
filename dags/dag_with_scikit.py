from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'jthuo',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5),
    
}



def get_scikitlearn():
    import sklearn
    print(f"Scikit-learn with version : {sklearn.__version__}")

def get_matplot():
    import matplotlib
    print(f"Matplotlib with version : {matplotlib.__version__}")

    
with DAG (dag_id='dependencies_v2', default_args=default_args, description="This is our first Python Operator DAG",
    start_date = datetime(2023, 12, 31), schedule_interval='@daily') as dag: 
    
    task1 = PythonOperator(
        task_id = "scikit-version",
        python_callable = get_scikitlearn,
    )

    task2 = PythonOperator(
        task_id = "matplotlib-version",
        python_callable = get_matplot,
    )
    
    task1 >> task2
    
    