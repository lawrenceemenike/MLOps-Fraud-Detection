from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import random
import logging

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_drift(**context):
    """
    Check for data drift.
    Returns True if drift is detected, False otherwise.
    """
    logger.info("Checking for data drift...")
    # Placeholder: Randomly detect drift
    drift_detected = random.random() < 0.1 # 10% chance
    
    if drift_detected:
        logger.warning("Drift DETECTED! Triggering retraining...")
        return "drift_detected"
    else:
        logger.info("No drift detected.")
        return "no_drift"

with DAG(
    'drift_detection_monitor',
    default_args=default_args,
    description='Monitor for data drift and trigger retraining',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    drift_check = PythonOperator(
        task_id='check_drift',
        python_callable=check_drift,
    )

    trigger_retrain = TriggerDagRunOperator(
        task_id='trigger_retrain',
        trigger_dag_id='train_and_register',
    )

    # Logic to trigger only if drift detected would typically use BranchPythonOperator
    # For simplicity here, we just show the DAG structure. 
    # To implement conditional logic properly:
    
    from airflow.operators.python import BranchPythonOperator
    
    branch_task = BranchPythonOperator(
        task_id='branch_drift',
        python_callable=check_drift,
    )
    
    no_drift_task = PythonOperator(
        task_id='no_drift',
        python_callable=lambda: logger.info("Ending workflow, no drift.")
    )

    drift_check >> branch_task
    branch_task >> trigger_retrain
    branch_task >> no_drift_task
