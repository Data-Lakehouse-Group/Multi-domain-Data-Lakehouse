"""
Taxi Pipeline DAG
==================
Orchestrates the full Bronze → Silver → Gold pipeline
for the NYC Taxi dataset.

Schedule: 1st of every month at midnight
Each run processes one month of data based on the execution date.
"""

import os
from datetime import datetime, timedelta
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# DEFAULT ARGS FOR PIPELINE ORCHESTRATION
# ---------------------------------------------------------------------------
default_args = {
    "owner"           : "lakehouse",
    "retries"         : None,
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------------
with DAG(
    dag_id          = "taxi_pipeline",
    description     = "NYC Taxi Bronze → Silver → Gold pipeline (With Validation and Download)",
    default_args    = default_args,
    start_date      = datetime(2023, 1, 1),
    end_date        = datetime(2023, 1, 1),
    schedule        = "0 0 1 * *",   # 1st of every month at midnight
    catchup         = True,          # backfill all months from start_date
    tags            = ["taxi", "lakehouse"],
) as dag:

    # -----------------------------------------------------------------------
    # TASK 1 — Download raw Parquet from TLC website
    # -----------------------------------------------------------------------
    download = BashOperator(
        task_id      = "download",
        bash_command="""
            python /opt/airflow/ingestion/taxi/download.py \
                --year {{ execution_date.year }} \
                --month-start {{ execution_date.month }} \
                --month-end {{ execution_date.month }}
        """
    )

    # -----------------------------------------------------------------------
    # TASK 2 — Ingest raw data into Bronze Delta table
    # -----------------------------------------------------------------------
    bronze_ingest = BashOperator(
        task_id      = "bronze_ingest",
        bash_command="""
            python /opt/airflow/ingestion/taxi/bronze_ingestion.py \
                --year {{ execution_date.year }} \
                --month-start {{ execution_date.month }} \
                --month-end {{ execution_date.month }}
        """
    )

    # -----------------------------------------------------------------------
    # TASK 3 — Validate Bronze with Great Expectations
    # Stops pipeline if checks fail
    # -----------------------------------------------------------------------
    bronze_validation = BashOperator(
        task_id      = "bronze_validation",
        bash_command="""
            python /opt/airflow/quality/taxi/bronze_suite.py \
                --year {{ execution_date.year }} \
                --month-start {{ execution_date.month }} \
                --month-end {{ execution_date.month }}
        """
    )

    # -----------------------------------------------------------------------
    # TASK 4 — Transform Bronze → Silver
    # -----------------------------------------------------------------------
    silver_transform = BashOperator(
        task_id      = "silver_transform",
        bash_command="""
            python /opt/airflow/transformations/silver/taxi.py \
                --year {{ execution_date.year }} \
                --month-start {{ execution_date.month }} \
                --month-end {{ execution_date.month }}
        """
    )

    # -----------------------------------------------------------------------
    # TASK 5 — Validate Silver with Great Expectations
    # Stops pipeline if quality checks fail
    # -----------------------------------------------------------------------
    # silver_validation = BashOperator(
    #     task_id      = "silver_validation",
    #     bash_command = f"""
    #         python /opt/airflow/quality/validation/taxi/silver_suite.py \
    #             --year {YEAR} \
    #             --month-start {MONTH} \
    #             --month-end {MONTH}
    #     """,
    # )

    # -----------------------------------------------------------------------
    # TASK 6 — Run dbt staging + intermediate + gold models
    # Builds tables in lakehouse.duckdb temp file for gold ingest after this
    # -----------------------------------------------------------------------
   # dbt build --vars '{"year": 2023, "month": 1}' --select tag:taxi --target prod
    gold_transform = DockerOperator(
        task_id        = "gold_dbt_transform",
        image          = "multi-domain-data-lakehouse-dbt",
        container_name = "task_dbt_{{ execution_date.strftime('%Y%m') }}",
        command        = [
            "dbt", "build",
            "--target", "prod",
            "--select", "+tag:taxi",
            "--vars '{\"year\": {{ execution_date.year }}, \"month\": {{ execution_date.month }}}'"
        ],
        environment    = {
            "DBT_PROFILES_DIR"          : "/usr/app/dbt",
            "DBT_LOG_PATH"              : "/usr/app/tmp/dbt_logs",
            "DBT_TARGET_PATH"           : "/usr/app/tmp/dbt_target",
            "AWS_ENDPOINT_URL"          : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
            "AWS_ACCESS_KEY_ID"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "AWS_REGION"                : "us-east-1",
            "AWS_S3_FORCE_PATH_STYLE"   : "true"
        },
        mounts         = [
            Mount(
                source = "/opt/airflow/transformations/dbt",
                target = "/usr/app/dbt",
                type   = "bind",
            ),
        ],
        network_mode   = "multi-domain-data-lakehouse_lakehouse",
        auto_remove    = "success",
        docker_url     = "unix://var/run/docker.sock",
        mount_tmp_dir  = False,
    )

    # -----------------------------------------------------------------------
    # TASK 7 — Write Gold Delta tables to MinIO
    # Reads dbt tables from lakehouse.duckdb temp file
    # -----------------------------------------------------------------------
    gold_ingest = BashOperator(
        task_id      = "gold_ingest",
        bash_command="""
            python /opt/airflow/ingestion/taxi/gold_ingestion.py \
                --year {{ execution_date.year }} \
                --month {{ execution_date.month }} 
        """
    )

    # -----------------------------------------------------------------------
    # PIPELINE ORDER
    # Each >> means "run after"
    # If any task fails everything downstream stops automatically
    # -----------------------------------------------------------------------
    (
        download
        >> bronze_ingest
        >> bronze_validation
        >> silver_transform
        # >> silver_validation
        >> gold_transform
        >> gold_ingest
    )