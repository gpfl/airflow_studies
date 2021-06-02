from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
import random

args = {
    'owner': 'gpfl',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='my_sample_dag', default_args=args, schedule_interval=None)


def print_hi(**context):
    received_value = context['ti'].xcom_pull(key='random_value')
    print(f'hi, i received the following {str(received_value)}')


def print_hello(**context):
    received_value = context['ti'].xcom_pull(key='random_value')
    print(f'hello, i received the following {str(received_value)}')


def push_to_xcom(**context):
    rnd = random.uniform(0, 1)
    context['ti'].xcom_push(key='random_value', value=rnd)
    print('i am okay')


def branch_func(**context):
    if random.random() > 0.5:
        return 'say_hi_task'
    return 'say_hello_task'



with dag:
    run_this_task = PythonOperator(
        task_id='run_this',
        python_callable=push_to_xcom,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )

    branch_op = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=branch_func
    )

    run_this_task_2 = PythonOperator(
        task_id='say_hi_task',
        python_callable=print_hi,
        provide_context=True
    )

    run_this_task_3 = PythonOperator(
        task_id='say_hello_task',
        python_callable=print_hello,
        provide_context=True
    )

    run_this_task >> branch_op >> [run_this_task_2, run_this_task_3]
