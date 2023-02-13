# Interaction with postgres in airflow

# run the code >> pip install apache-airflow-providers-postgres
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from airflow.operators.postgres_operator import PostgresOperator
import time

from airflow.utils.dates import days_ago


default_dag_args = {
    'owner': 'airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

create_query = """ 
DROP TABLE IF EXISTS public.stock_market_daily;
CREATE TABLE public.stock_market_daily (id INT NOT NULL, ticker VARCHAR(250), price_open FLOAT);
"""

# create a logic that populates the table with some data
insert_data_query = """
INSERT INTO public.stock_market_daily(id, ticker, price_open)
values(1, 'IBM' ,100.0 ),(2, 'IBM' ,200.0 ),(3, 'IBM' ,300.0 ),
(4, 'TSLA' ,400.0 ),(5, 'TSLA' ,500.0 ),(6, 'TSLA' ,600.0 )

"""

create_grouped_table = """
DROP TABLE IF EXISTS ticker_aggregated_data;
CREATE TABLE IF NOT EXISTS ticker_aggregated_data AS 
SELECT ticker, avg(price_open) FROM stock_market_daily
GROUP BY 1;
"""


with DAG(dag_id = "postgres_dag_connection", default_args = default_dag_args, schedule_interval = None, start_date = days_ago(1)) as postgres_dag:
    task_0 = PostgresOperator(task_id = "create_table", sql = create_query, postgres_conn_id = "postgresql_mui_local") 
    task_1 = PostgresOperator(task_id = "insert_data", sql = insert_data_query, postgres_conn_id = "postgresql_mui_local")
    task_2 = PostgresOperator(task_id = "group_data", sql = create_grouped_table, postgres_conn_id = "postgres_mui_local")


    task_0 >> task_1 >> task_2