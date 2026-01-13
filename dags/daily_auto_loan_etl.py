import sys
from datetime import datetime, timedelta

# 1. Add the ETL module path (Modularizing business logic)
sys.path.append('/opt/airflow/etl')

from airflow import DAG
from airflow.operators.python import PythonOperator

# 2. Import the ETL function from main.py
# Decoupling scheduling (Airflow) from data processing logic (main.py)
from main import run_etl

# 3. Default arguments for DAG
# Configured for production stability: automatic retries for transient network issues
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 4. Define the DAG for daily auto-loan ETL
# Implemented 'catchup=False' to ensure clean execution and prevent redundant historical processing
with DAG(
    dag_id='daily_auto_loan_etl',
    default_args=default_args,
    schedule_interval=None,   # Set to None for manual trigger; scalable to cron as needed
    catchup=False,
    description='Automated pipeline for ingesting and processing auto-loan data',
    tags=['etl', 'auto-loan', 'daily']
) as dag:

    # 5. Define the Python task that runs the ETL
    # Orchestrating the core ETL function through Airflow's PythonOperator
    run_etl_task = PythonOperator(
        task_id='run_etl_main',
        python_callable=run_etl 
    )

    # Task dependency definition
    run_etl_task