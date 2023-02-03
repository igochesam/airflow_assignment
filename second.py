from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

# first we write here our python logic
# create a dag with one python task only. This function should print the current datetime

def python_first_function():
    current_time = datetime.datetime.now()
    print('the current time is: ', current_time)


# create the DAG which calls the python logic that you have created

default_dag_args = {
    'start_date': datetime(2022,9,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}


with DAG('first_python_dag', schedule_interval = '@daily', catchup = False, default_args = default_dag_args) as dag_python:

    #define our tasks

    task_0 = PythonOperator(task_id = 'first_python_task', python_callable = python_first_function)