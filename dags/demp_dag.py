from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json

_SUCCESS_STATES = ['success', 'skipped']
_EXCEPTION_MSG_LIMIT = 10 * 1024  # 10kb

def on_success_callback_dag(context):
    dag_run = context.get('dag_run')
    print(f'DAG FAILURE: Dag with Dag run {dag_run} Failed.')

def on_failure_callback_dag(context):
    print(context)
    dag = context['dag']
    # exp = context['exception']
    # print(exp)
    print("demo exp")
    dag_run = context['dag_run']
    task_instances = dag_run.get_task_instances()
    dag_id=dag.dag_id
    run_id=context['run_id']
    success=dag_run.state in _SUCCESS_STATES
    # reason=context['reason']
    # tasks=[cls._get_task_instance_result(ti) for ti in task_instances]
    state=dag_run.state
    execution_date=dag_run.execution_date
    start_date=dag_run.start_date
    end_date=dag_run.end_date

    for ti in task_instances:
        print(ti)
        print(ti.task_id)
        print(ti.state)
        print(ti.log_url)

        data = requests.get("https://localhost:8080/api/v1/dags/dag_with_templated_dir/dagRuns/manual__2023-12-04T11:47:36.416879+00:00/taskInstances/my_task/logs/1?full_content=false",auth=("admin", "1234"))
        print(data)
        print(ti.prev_attempted_tries)
        print(ti.duration)
        print(ti.end_date)
        print(ti.start_date)
        print(ti.max_tries)
        print(ti.try_number)



    print(dag_run)
    print(dag)
    print(task_instances)
    print(dag_id)
    print(run_id)
    print(success)
    # print(reason)
    print(state)
    print(execution_date)
    print(start_date)
    print(end_date)
    
    print(f'DAG SUCCES: Dag with Dag run {dag_run} Failed.')

def on_success_callback_task(context):
    dag_run = context.get('dag_run')
    print(context)
    print(f'TASK SUCCES: Dag with Dag run {dag_run} Failed.')

def on_failure_callback_task(context):
    print(context)
    ex123 = context['exception']
    print("demo")
    print(ex123)
    dag_run = context.get('dag_run')
    ex = context.get('yesterday_ds_nodash')
    print(f'exception wdadd {ex}' )
    print(dag_run)
    print(context)
    task_instances = dag_run.get_task_instances()
    exception_message = context.get('exception') 
    print(exception_message)
    print(f'Specific Task Failure: Task {task_instances} failed for {exception_message} dag run {dag_run}.')

def on_failure_callback_task_args(context):
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    print(f'Generic Failure: Task {task_instances} failed for dag run {dag_run}.')

def failure_func():
    raise ValueError('File not parsed completely/correctly')

def success_func():
    print("sucessfully runed")

def on_execute_callback(context):
    print("on execute")
    print(context)

dag = DAG(
    dag_id='dag_with_templated_dir',
    start_date=datetime(2023,11,28),
    on_failure_callback=on_failure_callback_dag,
    catchup=False,
    max_active_runs=1,
    default_args={
        'on_execute_callback': on_execute_callback,
        'on_failure_callback': on_failure_callback_dag,
        'on_success_callback': on_success_callback_dag
    }
)

bash_task = BashOperator(
    task_id='my_task',
    bash_command='echo somethinh',
    dag=dag,
    # on_success_callback=on_success_callback_task
)

python_task = PythonOperator(
    task_id = 'my_python_task_1',
    python_callable=failure_func,
    # on_failure_callback=on_failure_callback_task,
    # on_success_callback=on_success_callback_task,
    dag=dag
)

python_leaf_task = PythonOperator(
    task_id='my_python_task',
    python_callable=failure_func,
    # on_failure_callback=on_failure_callback_task,
    # on_success_callback=on_success_callback_task,
    dag=dag,

    
)

bash_task >> python_task >> python_leaf_task
