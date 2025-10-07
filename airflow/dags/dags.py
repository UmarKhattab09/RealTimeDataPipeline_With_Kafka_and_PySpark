# File: dags/spark_etl_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for DAG
default_args = {
    'owner': 'Umar',
    'depends_on_past': False,
    'email': ['ru.khattab@gmail.com'],
    'email_on_failure': False,  # set True if you configure SMTP
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'spark_etl_dag',
    default_args=default_args,
    description='Local testing DAG: producer + transform',
    schedule_interval='*/1 * * * *',  # every minute for testing
    start_date=datetime(2025, 10, 6),
    catchup=False
)

# Task: run producer.py (simulate data)
run_producer = BashOperator(
    task_id='run_producer',
    bash_command='python "/opt/airflow/scripts/producer.py"',
    dag=dag
)

# Task: run transform.py (batch ETL)
run_transform = BashOperator(
    task_id='run_transform',
    bash_command='python "/opt/airflow/scripts/transform_stream.py"',
    dag=dag
)

# Set dependencies: producer first, then transform
run_producer >> run_transform
