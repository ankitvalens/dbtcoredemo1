from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

_SUCCESS_STATES = ['success', 'skipped']

def on_failure_callback_dag(context):
    dag_run = context.get('dag_run')
    print(f'DAG FAILURE: Dag with Dag run {dag_run} Failed.')

def on_success_callback_dag(context):
    print(context)
    dag = context['dag']
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
    dag_run = context.get('dag_run')
    print(context)
    task_instances = dag_run.get_task_instances()
    print(f'Specific Task Failure: Task {task_instances} failed for dag run {dag_run}.')

def on_failure_callback_task_args(context):
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    print(f'Generic Failure: Task {task_instances} failed for dag run {dag_run}.')

def failure_func():
    raise ArithmeticError()

def success_func():
    print("sucessfully runed")

dag = DAG(
    dag_id='dag_with_templated_dir',
    start_date=datetime(2023,11,28),
    on_failure_callback=on_failure_callback_dag,
    catchup=False,
    max_active_runs=1,
    default_args={
        'on_failure_callback': on_success_callback_dag,
        'on_success_callback': on_success_callback_dag
    }
)

bash_task = BashOperator(
    task_id='my_task',
    bash_command='echo somethinh',
    dag=dag,
    on_success_callback=on_success_callback_task
)

python_task = PythonOperator(
    task_id = 'my_python_task_1',
    python_callable=success_func,
    on_failure_callback=on_failure_callback_task,
    on_success_callback=on_success_callback_task,
    dag=dag
)

python_leaf_task = PythonOperator(
    task_id='my_python_task',
    python_callable=failure_func,
    dag=dag
)

bash_task >> python_task >> python_leaf_task
