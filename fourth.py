import requests
from datetime import datetime,timedelta
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_dag_args = {
    "start_date" : datetime(2023,1,1),
    "email_on_failure": False,
    "email_on_try": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "project_id": 1
}

def get_api():
    api_key = "XFSRJBR4X6ZVUAIO"
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=IBM&apikey=XFSRJBR4X6ZVUAIO'
    response = requests.get(url)
    path = "C:/Users/mkaya/kayamui_airflow/DATA_CENTER/DATA_LAKE/"
    try:
        wget.download(url, "stock_market").json()
    except:
        return None

with DAG("get_stock", schedule_interval = None, default_args = default_dag_args) as get_monthly:
    task_0 = PythonOperator(task_id = "get_data", python_callable=get_api)