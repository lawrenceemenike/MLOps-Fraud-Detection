from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
import psycopg2

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_batch_scoring():
    logger.info("Starting batch scoring...")
    
    # Database connection
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow_password"
    )
    
    # Load data from yesterday (simulated as 'all data' for this demo)
    query = "SELECT * FROM transactions"
    df = pd.read_sql(query, conn)
    
    logger.info(f"Loaded {len(df)} transactions from database.")
    
    if not df.empty:
        # Simulate batch scoring (re-scoring)
        # In reality, you'd load a specific model version here
        df['batch_score'] = df['amount'].apply(lambda x: 0.9 if x > 3000 else 0.1)
        
        # Save results (e.g., to a new table or file)
        # For demo, we just log the stats
        fraud_count = df[df['batch_score'] > 0.5].shape[0]
        logger.info(f"Batch scoring complete. Found {fraud_count} potential frauds.")
    else:
        logger.info("No data found to score.")
        
    conn.close()

with DAG(
    'daily_batch_scoring',
    default_args=default_args,
    description='Daily batch scoring of historical data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    scoring_task = PythonOperator(
        task_id='batch_score',
        python_callable=run_batch_scoring,
    )

    scoring_task
