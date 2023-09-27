from pendulum import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from cosmos.profiles import DatabricksTokenProfileMapping
from cosmos import DbtDag, LoadMode, RenderConfig, DbtTaskGroup, ProfileConfig, ProjectConfig
from cosmos.constants import TestBehavior
from airflow.operators.bash import BashOperator
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import re
import json
import os
import shutil

#PROJECT_ROOT_PATH="/opt/airflow/git/jaffle_shop.git/dags/dbt/jaffle_shop"  --> managed airflow path
#PROJECT_ROOT_PATH="/home/gopal/dbt-workspace/jaffle_shop/dags/dbt/jaffle_shop"  --> local development path
PROJECT_ROOT_PATH=Variable.get("PROJECT_ROOT_PATH")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="default",
    #If you are using profiles.yml file in git use below profiles_yml_filepath
    #profiles_yml_filepath=f"{PROJECT_ROOT_PATH}/profiles.yml",
    #here we are using Airflow connection to provide profile details
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id = 'jaffle_shop_databricks_connection',
    )
)

def my_function():
    conn = BaseHook.get_connection('jaffle_shop_databricks_connection')
    logging.info(conn)
    logging.info(conn.get_extra())
    logging.info(conn.get_uri())
    pattern = r"databricks://([a-zA-Z0-9.-]+)"

    # Use re.search to find the match
    match = re.search(pattern, conn.get_uri())
    if match:
        logging.info(match.group(1))
    return "done"

def generate_cred():
    credentials = {
        'credential1': 'value1',
        'credential2': 'value2',
        'credential3': 'value3',
    }
    current_directory = os.getcwd()


# List all files and directories in the current directory

    with open('credentials.json', 'w') as file:
        json.dump(credentials, file)  

    current_directory = os.getcwd() + '/credentials.json'
    dest_dir = '/tmp/'
    shutil.copy(current_directory, dest_dir)   

    file_list = os.listdir('/tmp/')

# Print the list of files and directories
    for item in file_list:
        print(item) 


    # for root, _, files in os.walk('/'):
    #     if filename in files:
    #         return os.path.join(root, filename)


dbt_var = '{{ ds }}'
with DAG(
        dag_id="1_jaffle_shop_dbt_3_22",
        start_date=datetime(2023, 9, 6),
        schedule="@daily",
        catchup=True
):
    e1 = EmptyOperator(task_id="pre_dbt")

    t1 = PythonOperator(
        task_id='print',
        python_callable= generate_cred,
    )

    run_this = BashOperator(
        task_id="run_after_loop1",
        bash_command="ls -l",
    )


    run_this_2 = BashOperator(
        task_id="run_after_loop",
        bash_command="mycliapp",
    )
   
    e2 = EmptyOperator(task_id="post_dbt")


    e1 >> t1 >> run_this >> run_this_2 >> e2
