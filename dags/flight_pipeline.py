from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from upload_to_s3 import fetch_and_upload_all
from dbt_runner import dbt_run,dbt_test

SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG(
    dag_id="flight_daily_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    fetch_upload = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_and_upload_all
    )
    create_stages = SnowflakeOperator(
        task_id="create_s3_stages",
        snowflake_conn_id="snowflake_default",
        sql=[
            """
            CREATE OR REPLACE STAGE RAW.AIRLINES_STAGE
            URL='s3://flight-project-bucket-smit/metadata/airlines.dat'
            CREDENTIALS = (
                AWS_KEY_ID='{{ conn.aws_default.login }}',
                AWS_SECRET_KEY='{{ conn.aws_default.password }}'
            )
            FILE_FORMAT = (FORMAT_NAME = RAW.FF_METADATA);
            """,
            """
            CREATE OR REPLACE STAGE RAW.AIRPORTS_STAGE
            URL='s3://flight-project-bucket-smit/metadata/airports.dat'
            CREDENTIALS = (
                AWS_KEY_ID='{{ conn.aws_default.login }}',
                AWS_SECRET_KEY='{{ conn.aws_default.password }}'
            )
            FILE_FORMAT = (FORMAT_NAME = RAW.FF_METADATA);
            """,
            """
            CREATE OR REPLACE STAGE RAW.WEATHER_STAGE
            URL='s3://flight-project-bucket-smit/weather/'
            CREDENTIALS = (
                AWS_KEY_ID='{{ conn.aws_default.login }}',
                AWS_SECRET_KEY='{{ conn.aws_default.password }}'
            )
            FILE_FORMAT = (FORMAT_NAME = RAW.FF_WEATHER_BTS);
            """,
            """         
            CREATE OR REPLACE STAGE RAW.BTS_STAGE
            URL = 's3://flight-project-bucket-smit/bts/'
            CREDENTIALS = (
                AWS_KEY_ID='{{ conn.aws_default.login }}',
                AWS_SECRET_KEY='{{ conn.aws_default.password }}'
            )
            FILE_FORMAT = (FORMAT_NAME = RAW.FF_WEATHER_BTS);
            """
        ]
    )


    load_airlines = SnowflakeOperator(
        task_id="load_airlines",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="COPY INTO RAW.AIRLINES_RAW FROM @RAW.AIRLINES_STAGE FILE_FORMAT=(FORMAT_NAME=RAW.FF_METADATA) FORCE = TRUE ON_ERROR='CONTINUE';"
    )

    load_airports = SnowflakeOperator(
        task_id="load_airports",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="COPY INTO RAW.AIRPORTS_RAW FROM @RAW.AIRPORTS_STAGE FILE_FORMAT=(FORMAT_NAME=RAW.FF_METADATA) FORCE = TRUE ON_ERROR='CONTINUE';"
    )

    load_weather = SnowflakeOperator(
        task_id="load_weather",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="COPY INTO RAW.WEATHER_RAW FROM @RAW.WEATHER_STAGE FILE_FORMAT=(FORMAT_NAME=RAW.FF_WEATHER_BTS) FORCE = TRUE ON_ERROR='CONTINUE';"
    )

    load_bts = SnowflakeOperator(
        task_id="load_bts",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            COPY INTO RAW.BTS_RAW
            FROM @RAW.BTS_STAGE
            PATTERN='.*\\.zip'
            FILE_FORMAT=(FORMAT_NAME = RAW.FF_WEATHER_BTS)
            FORCE=TRUE
            ON_ERROR='CONTINUE';
        """
    )

    run_dbt = PythonOperator(
        task_id="run_dbt_models",
        python_callable=dbt_run
    )

    run_dbt_test = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=dbt_test
    )
    
    fetch_upload >> create_stages >> [load_airlines, load_airports, load_weather, load_bts] >> run_dbt >> run_dbt_test