from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

args = {
    'owner': 'gpfl',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='my_sample_filesensor', default_args=args, schedule_interval=None)


def say_hi(**context):
    print('hi')


with dag:
    sensing_task = FileSensor(
        task_id='sensing_task',
        filepath='test.txt',
        fs_conn_id='my_file_system',
        poke_interval=10
    )

    run_this_task = PythonOperator(
        task_id='run_this',
        python_callable=say_hi,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )

    sensing_task >> run_this_task
