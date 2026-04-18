import os
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# --- SQL queries ---
create_stage_sql = """
CREATE OR REPLACE STAGE DEMO.DEMO_SCHEMA.weather_stage
    url='s3://snowflake-workshop-lab/weather-nyc'
    FILE_FORMAT=(TYPE=JSON);
"""

create_weather_table_sql = """
CREATE OR REPLACE TABLE DEMO.DEMO_SCHEMA.WEATHER (
    CITYNAME STRING,
    LAT FLOAT,
    LON FLOAT,
    CLOUDS INTEGER,
    HUMIDITY INTEGER,
    PRESSURE FLOAT,
    TEMP FLOAT,
    TIME TIMESTAMP,
    WEATHER STRING
);
"""

copy_into_weather_sql = """
COPY INTO DEMO.DEMO_SCHEMA.WEATHER
FROM (
    SELECT 
        t.$1:city:findname,
        t.$1:city:coord:lat,
        t.$1:city:coord:lon,
        t.$1:clouds:all,
        t.$1:main:humidity,
        t.$1:main:pressure,
        t.$1:main:temp,
        t.$1:time,
        t.$1:weather[0]:main
    FROM @DEMO.DEMO_SCHEMA.weather_stage t
);
"""

