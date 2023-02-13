import requests
import time
from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd

default_dag_args = {
    'start_date':datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'project_id':1
}

# To make sure your functions accept arguments, you must define keyword arguments -> kwargs

# By writing kwargs your airflow is expecting a dictionary or argument

def get_data(**kwargs):
    ticker = kwargs['ticker']
    api_key = "XFSRJBR4X6ZVUAIO"
    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=' + ticker +'&apikey='+api_key
    r = requests.get(url)
    path = "C:/Users/mkaya/kayamui_airflow/DATA_CENTER/DATA_LAKE/"
    data = r.json()
    try:
        with open(path + "stock_market_raw_data" + ticker + "_" + str(time.time()), "w") as outfile:
            json.dump(data, outfile)
    except:
        return "Didn't work properly"

def test_data(**kwargs):
    read_path = "C:/Users/mkaya/kayamui_airflow/DATA_CENTER/DATA_LAKE/"
    ticker = kwargs["ticker"]
    latest = np.max([float(file.split("_")[-1]) for file in os.listdir(read_path) if ticker in file])
    latest_file = [file for file in os.listdir(read_path) if str(latest) in file][0]

    file = open(read_path + latest_file )
    data = json.load(file)

    # This is our testing condition
    condition_1 = len(data.keys()) == 2
    condition_2 = "Weekly Time Series" in data.keys()
    condition_3 = "Meta Data" in data.keys()

    if condition_1 and condition_2 and condition_3:
        # for the branch operator we want to return the name of another task
        return  "clean_market_data"
    else:
        return "failed_task_data"


def clean_market_data(**kwargs):
    api_key = "XFSRJBR4X6ZVUAIO"
    CLEAN_DATA_PATH = "C:/Users/mkaya/kayamui_airflow/DATA_CENTER/CLEAN_DATA/"
    DATA_LAKE_PATH = "C:/Users/mkaya/kayamui_airflow/DATA_CENTER/DATA_LAKE"
    ticker = kwargs['ticker']
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol='+ticker+'&apikey=' + api_key
    r = requests.get(url)
    data = r.json()

    try:
        with open(DATA_LAKE_PATH + "Weekly IBM Snapshot_" +ticker + "_" + str(time.time()), "w") as f:
            f.write(data.content)
    except:
        return "Program gives nothing"

    clean_data = pd.DataFrame(data["Weekly Time Series"]).T
    clean_data["ticker"] = data["Meta Data"]["2. Symbol"]
    clean_data["meta_data"] = str(data["Meta Data"])
    clean_data["time_stamp"] = pd.to_datetime("now")

    #now make an output

    clean_data.to_csv(CLEAN_DATA_PATH + ticker + " Snapshopts Weekly" + str(pd.to_datetime('now')))



with DAG("market_data_alphavantage_dag", schedule_interval = None, default_args = default_dag_args) as get_IBM_weekly:
    task_0 = PythonOperator(task_id = 'get_IBM_market', python_callable = get_data, op_kwargs = {'ticker':"IBM"})
    task_1 = BranchPythonOperator(task_id = "test_market_data", python_callable= test_data, op_kwargs = {"ticker": "IBM"})
    
    task_2_1 = PythonOperator(task_id = 'clean_market_data', python_callable = clean_market_data, op_kwargs = {'ticker': "IBM"})
    task_2_2 = DummyOperator(task_id = "failed_task_data")

    task_3 = DummyOperator(task_id = "aggregate_all_data_to_db")

    task_0>>task_1
    task_1>>task_2_1>>task_3
    task_1>>task_2_2>>task_3