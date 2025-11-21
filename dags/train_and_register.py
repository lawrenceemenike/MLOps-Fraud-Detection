from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'train_and_register',
    default_args=default_args,
    description='Train and register fraud detection model',
    schedule_interval='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    train_task = BashOperator(
        task_id='train_model',
        bash_command='python /opt/airflow/services/training/train.py',
        env={
            'MLFLOW_TRACKING_URI': 'http://mlflow:5000'
        }
    )

    train_task
