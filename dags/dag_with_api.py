from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner' : 'jthuo',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5),
    
}

@dag(
    dag_id="dag_with_taskflow_api_v2",
    default_args=default_args,
    start_date=datetime(2023, 12, 31),
    schedule_interval="@daily"
)


# Simple etl to demonstrate api
def hello_world_etl(): 
    
    # Here we expect to have both first name and last name
    @task(multiple_outputs=True)
    def get_name():
        return {"first_name": "John",
                "last_name" : "Thuo"
                }
    
    @task()
    def get_age():
        return 19
    
    @task()
    def get_year():
        return "Three(3)"
    
    @task()
    def greet(first_name: str, last_name: str, age: int, year: str):
        print(f"Hey! My name is {first_name} {last_name}.I am {age} years old and in Year {year}!")
    
    output_dict = get_name()    
    age = get_age()
    year = get_year()
    greet(first_name=output_dict["first_name"], last_name=output_dict["last_name"], age=age, year=year)
    
greet_dag = hello_world_etl()