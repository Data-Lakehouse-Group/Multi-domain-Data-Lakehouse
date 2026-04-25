"""
Weather Pipeline DAG
====================
Orchestrates the Bronze → Silver pipeline for NOAA GSOD weather data.
Because the dataset is static, the pipeline is triggered manually.

Schedule: None — manual trigger only.
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_pipeline",
    description="NOAA Weather Bronze → Silver pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["weather", "lakehouse"],
) as dag:

    download = BashOperator(
        task_id="download_weather",
        bash_command="python /opt/airflow/ingestion/weather/download.py --year-start 2020 --year-end 2023",
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest_weather",
        bash_command="python /opt/airflow/ingestion/weather/ingestion.py --year-start 2020 --year-end 2023",
    )

    bronze_validation = BashOperator(
        task_id="bronze_validation_weather",
        bash_command="python /opt/airflow/quality/bronze/weather_bronze_validation.py --year-start 2020 --year-end 2023",
    )

    silver_transform = BashOperator(
        task_id="silver_transform_weather",
        bash_command="python /opt/airflow/transformations/silver/weather.py",
    )

    silver_validation = BashOperator(
        task_id="silver_validation_weather",
        bash_command="python /opt/airflow/quality/silver/weather_silver_validation.py",
    )

    download >> bronze_ingest >> bronze_validation >> silver_transform >> silver_validation