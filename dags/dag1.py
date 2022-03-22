from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'end_date': datetime(2023, 1, 1),
    'email': ['mishalolhopengok@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue': 'bash_queue',
    'pool': 'backfill',
    'priority_weight': 10,
    'sla': timedelta(hours=2),
    'execution_timeout': timedelta(seconds=300),
    'trigger_rule': 'all_success'
}

dag = DAG(
    dag_id='example_branch_dop_operator_v3',
    default_args=default_args,
    description="A simple DAG",
    tags=['example']
)


def should_run(**kwargs):
    print('-exec dttm = {} and minute = {}'.
          format(kwargs['execution date'], kwargs['execution date'].minute))
    if kwargs['execution date'].minute % 2 == 0:
        return "dummy_task 1"
    else:
        return "dummy task 2"


cond = BranchPythonOperator(
    task_id='condition',
    provide_context=True,
    python_callable=should_run,
    dag=dag,
)
dummy_task_1 = DummyOperator(task_id='dummy_task_1', dag=dag)
dummy_task_2 = DummyOperator(task_id='dummy_task_2', dag=dag)
cond >> [dummy_task_1, dummy_task_2]
