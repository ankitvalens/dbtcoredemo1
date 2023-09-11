from pendulum import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from cosmos import ProfileConfig
from cosmos.profiles import DatabricksTokenProfileMapping

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
dbt_var = '{{ ds }}'
with DAG(
        dag_id="jaffle_shop_dbt_3_22",
        start_date=datetime(2023, 9, 6),
        schedule="@daily",
        catchup=True
):
    e1 = EmptyOperator(task_id="pre_dbt")

   
    e2 = EmptyOperator(task_id="post_dbt")


    e1 >> e2
