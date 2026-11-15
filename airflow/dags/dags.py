from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SCRIPTS = "/opt/airflow/scripts"

default_args = {
    'owner': 'Umar',
    'depends_on_past': False,
    'email': ['ru.khattab@gmail.com'],
    'email_on_failure': False,  # set True if you configure SMTP
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'spark_etl_dag',
    default_args=default_args,
    description='Kafka topics -> Postgres raw tables -> analytics tables',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 10, 6),
    catchup=False,
    max_active_runs=1,  # the ingest job has one checkpoint, so runs must not overlap
) as dag:

    # The producer and the subscriber services run all the time as their own
    # containers. Airflow only runs the jobs that start, finish and repeat.

    # Drain everything new from the four Kafka topics into Postgres, then exit.
    run_spark_ingest = BashOperator(
        task_id='run_spark_ingest',
        bash_command=f'python {SCRIPTS}/spark_stream.py --once',
    )

    # Rebuild the analytics tables from the raw tables.
    run_transform = BashOperator(
        task_id='run_transform',
        bash_command=f'python {SCRIPTS}/transform.py',
    )

    run_spark_ingest >> run_transform
