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

def generate_cred():
    conn = BaseHook.get_connection('jaffle_shop_databricks_connection')
    extras_dict = json.loads(conn.get_extra())

    credentials = {
        'connection_string': str(conn.host),
        'token': str(extras_dict["token"]),
        'http_path': str(extras_dict["http_path"]),
    }

    with open('/tmp/credentials.json', 'w') as file:
        json.dump(credentials, file)  

    print(credentials)


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
