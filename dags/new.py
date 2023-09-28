from pendulum import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from cosmos.profiles import DatabricksTokenProfileMapping
from cosmos import ProfileConfig
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import json
import logging
import os

PROJECT_ROOT_PATH=Variable.get("PROJECT_ROOT_PATH")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="default",
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id = 'jaffle_shop_databricks_connection',
    )
)

def generate_cred():
    conn = BaseHook.get_connection('jaffle_shop_databricks_connection')
    databricks_extras_dict = json.loads(conn.get_extra())

    az_conn = BaseHook.get_connection('azure_monitor')
    azure_monitor_extras_dict = json.loads(az_conn.get_extra())
    credentials = {
        'connection_string': str(conn.host),
        'token': str(databricks_extras_dict["token"]),
        'http_path': str(databricks_extras_dict["http_path"]),
        'workspace_id':str(azure_monitor_extras_dict['workspace_id']),
        'primary_key':str(azure_monitor_extras_dict['primary_key'])
    }

    root_directory = '/opt/airflow/git/dbtcoredemo1.git/dags/dbt/jaffle_shop/'  # Use '/' to represent the root directory

    # List all files and directories in the root directory
    file_list = os.listdir(root_directory)

    logging.info(file_list)


with DAG(
        dag_id="1_jaffle_shop_dbt_3_22",
        start_date=datetime(2023, 9, 27),
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
