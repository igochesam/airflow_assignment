from airflow import DAG
import requests
import time
import json
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

import pandas as pd
import numpy as np
import os

# first we write here our python logic

def get_data():
    api_key = 'L4TARUM2IB6JJ8P0'
    url = 'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey='+api_key
    r = requests.get(url)

    try:
        data = r.json()
        path = 'C:\Users\User\Desktop\data_centre\data_lake'
        with open(path + 'stock_market_raw_data' + 'IBM_' + str(time,time()), "w") as outfile:
            json.dump(data, outfile)

    except:
        pass


# create the DAG which calls the python logic that you have created

default_dag_args = {
    'start_date': datetime(2022,9,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 1
}


with DAG('market_data_alphavantage_dag', schedule_interval = '@daily', catchup = False, default_args = default_dag_args) as dag_python:

    #define our tasks

    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'tickers' : []})