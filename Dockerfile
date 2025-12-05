FROM apache/airflow:2.9.0


USER airflow
RUN pip install --no-cache-dir \
        protobuf==4.25.3 \
        dbt-core \
        dbt-snowflake

USER root
RUN apt-get update && apt-get install -y git curl

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

