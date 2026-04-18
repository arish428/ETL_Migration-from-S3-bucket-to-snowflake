import sys
import os

# # Add the folder (not the file) to sys.path
# sys.path.append('/opt/Data_validation_QA/qa_data_validation/ETL_from_S3_to_Snowflake')

# from S3DataloadSnowflake import (
#     create_stage_sql,
#     copy_into_weather_sql,
#     create_weather_table_sql
# )

from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'arish khan',
    'start_date': datetime(2025, 6, 26),
    'retries': 0,
}

# Create the DAG
with DAG(
    dag_id='datavalidation_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # Runs every 5 minutes
    catchup=False,
    tags=['example', 'bash'],
) as dag:

    # Define a BashOperator task
    run_dummy_task1 = BashOperator (
        task_id='Issue_Severity_to_Temp',
        bash_command='bash /opt/Data_validation_QA/qa_data_validation/IssueSeverity_records_0.1/IssueSeverity_records/IssueSeverity_records_run.sh '
    )

        # Define a BashOperator task
    run_dummy_task2 = BashOperator(
        task_id='Issue_Severity_to_datasource',
        bash_command='bash /opt/Data_validation_QA/qa_data_validation/IssueSeverity_recordstodatasource_0.1/IssueSeverity_recordstodatasource/IssueSeverity_recordstodatasource_run.sh '
        
    )
    [run_dummy_task1, run_dummy_task2]

# with DAG(
#     dag_id="snowflake_weather_pipeline_env",
#     start_date=datetime(2025, 9, 1),
#     schedule_interval='*/5 * * * *',
#     catchup=False,
#     tags=["snowflake", "weather"],
# ) as snowflake_dag:

#     create_stage = SnowflakeOperator(
#         task_id="create_stage",
#         sql=create_stage_sql,
#         snowflake_conn_id="snowflake_conn",  # Use Airflow connection if configured
#     )

#     create_weather_table = SnowflakeOperator(
#         task_id="create_weather_table",
#         sql=create_weather_table_sql,
#         snowflake_conn_id="snowflake_conn",
#     )

#     copy_into_weather = SnowflakeOperator(
#         task_id="copy_into_weather",
#         sql=copy_into_weather_sql,
#         snowflake_conn_id="snowflake_conn",
#     )

#     create_stage >> create_weather_table >> copy_into_weather