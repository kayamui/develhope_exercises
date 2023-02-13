import requests
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

from airflow.utils.dates import days_ago

default_dag_args={
'start_date':datetime(2023, 2, 13),
'email_on_failure':False,
'email_on_retry':False,
'retries':1,
'retry_delay':timedelta(minutes=5),
'project_id':1
}

def choice_function():
    x = 10
    if x > 11:
        return task_4
    else:
        return task_final



"""
#sequence of tasks
with DAG(dag_id = "common_airflow_patterns", default_args = default_dag_args, schedule_interval = None) as new_dag:
    task_0 = DummyOperator(task_id = "first_task")
    task_1 = DummyOperator(task_id= "second_task")
    task_3 = DummyOperator(task_id= "third_task")
    task_4 = DummyOperator(task_id = "fourth_task")
    task_5 = DummyOperator(task_id = "fifth_task")

    task_0 >> task_1 >> task_3 >> task_4 >> task_5
"""
"""
#parallel split
with DAG(dag_id = "common_airflow_patterns", default_args = default_dag_args, schedule_interval = None) as new_dag:
    task_0 = DummyOperator(task_id = "task_0")
    task_1 = DummyOperator(task_id= "task_1")
    task_2 = DummyOperator(task_id= "task_2")
    task_3 = DummyOperator(task_id = "task_3")
    task_4 = DummyOperator(task_id = "task_4")

    task_0 >> task_1 >> [task_2 ,task_3 , task_4]
"""
"""
with DAG(dag_id = "common_airflow_patterns", default_args = default_dag_args, schedule_interval = None) as new_dag:
    task_0 = DummyOperator(task_id = "task_0")
    task_1 = DummyOperator(task_id= "task_1")
    task_2 = DummyOperator(task_id= "task_2")
    task_3 = DummyOperator(task_id = "task_3")
    task_4 = DummyOperator(task_id = "task_4")

    task_0 >> task_1 >> [task_2 ,task_3] >> task_4
"""

"""
#synchronizations 
with DAG(dag_id = "common_airflow_patterns", default_args = default_dag_args, schedule_interval = None) as new_dag:
    task_0 = DummyOperator(task_id = "task_0")
    task_1 = DummyOperator(task_id= "task_1")
    task_2 = DummyOperator(task_id= "task_2")
    task_3 = DummyOperator(task_id = "task_3")
    task_4 = DummyOperator(task_id = "task_4")
    task_5 = DummyOperator(task_id = "task_5")
    task_6= DummyOperator(task_id = "task_6")
    task_final = DummyOperator(task_id = "task_final")

    task_0 >> task_1 >> [task_2 ,task_3, task_4, task_5, task_6] >> task_final
"""
"""
#exclusive choices
with DAG(dag_id = "common_airflow_patterns", default_args = default_dag_args, schedule_interval = None) as new_dag:
    task_0 = BranchPythonOperator(task_id = "task_0")
    task_4 = BranchPythonOperator(task_id = "task_4")
    task_final = BranchPythonOperator(task_id = "task_final", python_callable= choice_function)

    task_0 >> task_4 >> task_final
"""
# dynamic task generations
with DAG(dag_id = "common_airflow_patterns", default_args = default_dag_args, schedule_interval = None) as new_dag:
    final_task = DummyOperator(task_id = "read_input_hour_{}".format(hour))
    for hour in range(0,10):
        input_task = DummyOperator(task_id = "generate_data_hour{}".format(hour))
        data_proccessing_task = DummyOperator(task_id = "generate_data_hour_{}".format(hour))
        input_task >> data_proccessing_task >> final_task
