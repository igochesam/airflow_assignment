from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_dag_args = {
    'start_date': datetime(2022,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}

#Define our DAG

with DAG('First_DAG', schedule_interval = None, default_args = default_dag_args) as dag:

    # At this level we define our task of the DAGs

    task_0 = BashOperator(task_id = 'bash_task', bash_command = 'echo "command executed from bash operator"')
    task_1 = BashOperator(task_id = 'bash_move_data', bash_command = 'cp C:\Users\User\Desktop\data_centre\data_lake\dataset_raw.txt C:\Users\User\Desktop\data_centre\clean_data')
    task_2 = BashOperator(task_id = 'bash_remove_data', bash_command = "rm C:\Users\User\Desktop\data_centre\data_lake\dataset_raw.txt")

    # task dependency

    task_0 >> task_1 >> task_2