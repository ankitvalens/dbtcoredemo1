from pendulum import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from cosmos import DbtDag, LoadMode, RenderConfig, DbtTaskGroup, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import TestBehavior
from airflow.operators.bash import BashOperator

#PROJECT_ROOT_PATH="/opt/airflow/git/jaffle_shop.git/dags/dbt/jaffle_shop"  --> managed airflow path
#PROJECT_ROOT_PATH="/home/gopal/dbt-workspace/jaffle_shop/dags/dbt/jaffle_shop"  --> local development path
PROJECT_ROOT_PATH=Variable.get("PROJECT_ROOT_PATH")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    #If you are using profiles.yml file in git use below profiles_yml_filepath
    #profiles_yml_filepath=f"{PROJECT_ROOT_PATH}/profiles.yml",
    #here we are using Airflow connection to provide profile details
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id = 'snowflake_conn'
    )
)

with DAG(
        dag_id="jaffle_shop_dbt_2",
        start_date=datetime(2023, 11, 2),
        schedule="@daily",
):
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(dbt_project_path=PROJECT_ROOT_PATH),
        profile_config=profile_config,
        operator_args={
            "env": {"DBT_TARGET_PATH": "/tmp/"},
        }
    )

    purview = BashOperator(
        task_id="purview",
        bash_command="dbtpurview --path=/tmp --env=snowflake --dwhcid=snowflake_conn",
        trigger_rule= "all_done"
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg >> purview >> e2
