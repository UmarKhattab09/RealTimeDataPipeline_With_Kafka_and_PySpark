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
    description='Click events: producer -> Kafka -> raw CSV -> transformed CSV',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 10, 6),
    catchup=False,
    max_active_runs=1,  # the Spark jobs share one checkpoint, so runs must not overlap
) as dag:

    # Send a fixed batch of events so the task finishes.
    run_producer = BashOperator(
        task_id='run_producer',
        bash_command=f'python {SCRIPTS}/producer.py --num-events 30 --interval 0.5',
    )

    # Drain everything new from Kafka into data/raw, then exit.
    run_spark_stream = BashOperator(
        task_id='run_spark_stream',
        bash_command=f'python {SCRIPTS}/spark_stream.py --once',
    )

    # Rebuild the cleaned events and aggregates in data/transformed.
    run_transform = BashOperator(
        task_id='run_transform',
        bash_command=f'python {SCRIPTS}/transform.py',
    )

    run_producer >> run_spark_stream >> run_transform
