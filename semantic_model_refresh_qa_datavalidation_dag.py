import sys
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import subprocess
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Loadsemanticmodel import run_func

# if __name__ == "__main__":
#     print("🔹 Running run_func() manually for testing...\n")
#     run_func()
    
# Define default arguments
default_args = {
    'owner': 'arish khan',
    'start_date': datetime(2025, 6, 26),
    'retries': 0,
}

# Create the DAG
with DAG(
    dag_id='Refresh_PowerBI_QA_Dashboard',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # Runs every 5 minutes
    catchup=False,
    tags=['semantic model', 'power bi'],
) as dag:

    run_powerbi_refresh = PythonOperator(
        task_id="run_powerbi_refresh",
        python_callable=run_func,
        provide_context=True
    )

    # Define DAG flow
    run_powerbi_refresh