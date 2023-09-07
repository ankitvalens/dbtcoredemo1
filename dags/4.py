from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from cosmos import DbtDag, LoadMode, RenderConfig, DbtTaskGroup, ProfileConfig, ProjectConfig
from cosmos.profiles import DatabricksTokenProfileMapping
from cosmos.constants import TestBehavior
from airflow.operators.python_operator import PythonOperator

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

with DAG(
        dag_id="jafdsffle_shop_dbt24",
        start_date=datetime(2023, 9, 5),
        schedule="@daily",
        catchup=True,
        default_args = {
            'depends_on_past': True
        }
):
    e1 = EmptyOperator(task_id="pre_dbt")

    # dbt_tg = DbtTaskGroup(
    #     project_config=ProjectConfig(PROJECT_ROOT_PATH),
    #     profile_config=profile_config,
    #     render_config=RenderConfig(
    #         select=['tag:customers'],
    #     )
    # )

    def check_and_retry(**kwargs):
        execution_date = kwargs['execution_date']
        current_date = datetime.now().date()

        if 1 < 2:
            # Retry the task for the current date
            raise Exception("Task failed for the current date, retrying...")
        else:
            # Task for future dates
            print(f"Running task for {execution_date}")

    retry_and_advance_task = PythonOperator(
        task_id='retry_and_advance_task',
        python_callable=check_and_retry,
        provide_context=True,
    )


    e2 = EmptyOperator(task_id="post_dbt")


    e1 >> retry_and_advance_task >> e2
