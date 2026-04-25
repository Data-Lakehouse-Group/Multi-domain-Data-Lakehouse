"""
Retail Pipeline DAG
===================
Orchestrates the full Bronze → Silver pipeline for Instacart Retail data.
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
    dag_id="retail_pipeline",
    description="Instacart Retail Bronze → Silver pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["retail", "lakehouse"],
) as dag:

    download = BashOperator(
        task_id="download_retail",
        bash_command="python /opt/airflow/ingestion/retail/download.py",
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest_retail",
        bash_command="python /opt/airflow/ingestion/retail/ingestion.py",
    )

    bronze_validation = BashOperator(
        task_id="bronze_validation_retail",
        bash_command="python /opt/airflow/quality/bronze/retail_bronze_validation.py",
    )

    silver_transform = BashOperator(
        task_id="silver_transform_retail",
        bash_command="python /opt/airflow/transformations/silver/retail.py",
    )

    silver_validation = BashOperator(
        task_id="silver_validation_retail",
        bash_command="python /opt/airflow/quality/silver/retail_silver_validation.py",
    )

    download >> bronze_ingest >> bronze_validation >> silver_transform >> silver_validation