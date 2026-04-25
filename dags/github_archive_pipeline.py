"""
GitHub Archive Pipeline DAG
===========================
Orchestrates the Bronze → Silver pipeline for GitHub Archive data.
This dataset is static, so the pipeline processes a single date range
defined by the DAG run's configuration or defaults.

Schedule: None (trigger manually) — data is static.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# DEFAULT ARGS
# ---------------------------------------------------------------------------
default_args = {
    "owner": "lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------------
with DAG(
    dag_id="github_archive_pipeline",
    description="GitHub Archive Bronze → Silver pipeline",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["github", "lakehouse"],
) as dag:

    # -------------------------------------------------------------------
    # TASK 1 — Download raw JSON.GZ from GH Archive
    # -------------------------------------------------------------------
    download = BashOperator(
        task_id="download",
        bash_command="""
            python /opt/airflow/ingestion/github_archive/download.py \
                --date-hour-start {{ dag_run.conf.get('date_hour_start', '2023-02-01-0') }} \
                --date-hour-end {{ dag_run.conf.get('date_hour_end', '2023-02-01-0') }}
        """
    )

    # -------------------------------------------------------------------
    # TASK 2 — Ingest raw data into Bronze Delta table
    # -------------------------------------------------------------------
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="""
            python /opt/airflow/ingestion/github_archive/ingestion.py \
                --date-hour-start {{ dag_run.conf.get('date_hour_start', '2023-02-01-0') }} \
                --date-hour-end {{ dag_run.conf.get('date_hour_end', '2023-02-01-0') }}
        """
    )

    # -------------------------------------------------------------------
    # TASK 3 — Validate Bronze with Great Expectations
    # -------------------------------------------------------------------
    bronze_validation = BashOperator(
        task_id="bronze_validation",
        bash_command="""
            python /opt/airflow/quality/bronze/github_bronze_validation.py \
                --date-hour-start {{ dag_run.conf.get('date_hour_start', '2023-02-01-0') }} \
                --date-hour-end {{ dag_run.conf.get('date_hour_end', '2023-02-01-0') }}
        """
    )

    # -------------------------------------------------------------------
    # TASK 4 — Transform Bronze → Silver
    # -------------------------------------------------------------------
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="python /opt/airflow/transformations/silver/github.py"
    )

    # -------------------------------------------------------------------
    # TASK 5 — Validate Silver with Great Expectations
    # -------------------------------------------------------------------
    silver_validation = BashOperator(
        task_id="silver_validation",
        bash_command="python /opt/airflow/quality/silver/github_silver_validation.py"
    )

    # -------------------------------------------------------------------
    # PIPELINE ORDER
    # -------------------------------------------------------------------
    download >> bronze_ingest >> bronze_validation >> silver_transform >> silver_validation