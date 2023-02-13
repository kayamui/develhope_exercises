from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
# an operator is airflow's way of talking to some sort of language
# python operator
# posgtres operator
# bashoperator (Airflowa falanca bash command'i çalıştır der)

default_dag_args={
    'start_date': datetime(2022,1,17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
    'project_id':1
}

# lets define our dag
# isim, çalışma aralığı, default işlemler 
with DAG("First_DAG", schedule_interval = None, default_args = default_dag_args) as dag:

    # here at his level we define our tasks
    task_0 = BashOperator(task_id = 'bash_task', bash_command= "echo 'command executed from bash operator'")
    
    #şimdi txt dosyamızı alıp başka yere kopyalayalım
    task_1 = BashOperator(task_id = 'bash_task_move_data', bash_command= "cp C://Users/mkaya/kayamui_airflow/DATA_CENTER/DATA_LAKE/data.txt C://Users/mkaya/kayamui_airflow/DATA_CENTER/CLEAN_DATA/data.txt")

    # Önce task_0 yürütülsün sonra da task_1 yürütülsün istiyorum
    task_0 >> task_1
   
   
   
    #challenge: make a command that create a folder mkdir some_folder_name
    #task_1 = BashOperator(task_id = "create_directory", bash_command ='mkdir "C:/Users/mkaya/OneDrive/Masaüstü/"')