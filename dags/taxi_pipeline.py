"""
Taxi Pipeline DAG
==================
Orchestrates the full Bronze → Silver → Gold pipeline
for the NYC Taxi dataset.

Schedule: 1st of every month at midnight
Each run processes one month of data based on the execution date.
"""

import os
import json
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.models import Connection



# ---------------------------------------------------------------------------
# DEFAULT ARGS FOR PIPELINE ORCHESTRATION
# ---------------------------------------------------------------------------
default_args = {
    "owner"           : "lakehouse",
    "retries"          : 2,           #Retry until successs in prod
    "retry_delay"      : timedelta(minutes=1), #Retries everyday in prod
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DBT Server Connection
# ---------------------------------------------------------------------------

def create_dbt_connection():
    conn = Connection(
        conn_id   = "dbt_server",
        conn_type = "http",
        host      = "lakehouse-dbt",
        port      = 8001,
        schema    = "http",
    )
    session = settings.Session()
    existing = session.query(Connection).filter(
        Connection.conn_id == "dbt_server"
    ).first()
    if not existing:
        session.add(conn)
        session.commit()

create_dbt_connection()

# ---------------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------------
with DAG(
    dag_id          = "taxi_pipeline",
    max_active_runs =1,              #Needed so that gold layer works in sync and temp files arent over written
    max_active_tasks=1,
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
    silver_validation = BashOperator(
        task_id      = "silver_validation",
        bash_command = """
            python /opt/airflow/quality/taxi/silver_suite.py \
                --year {{ execution_date.year }} \
                --month-start {{ execution_date.month }} \
                --month-end {{ execution_date.month }}
        """,
    )

    # -----------------------------------------------------------------------
    # TASK 6 — Run dbt staging + intermediate + gold models
    # Calls on an external dbt server that stores its transforms as a parquet
    # first to MinIO for the gold ingest to then convert this
    # to a delta table
    # -----------------------------------------------------------------------
    gold_transform = SimpleHttpOperator(
        task_id         = "gold_transform",
        http_conn_id    = "dbt_server",
        endpoint        = "/taxi",
        method          = "POST",
        data            = json.dumps({
            "year"  : "{{ execution_date.year }}",
            "month" : "{{ execution_date.month }}"
        }),
        headers         = {"Content-Type": "application/json"},
        response_check  = lambda response: response.json()["success"] == True,
        log_response    = True,
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
        >> silver_validation
        >> gold_transform
        >> gold_ingest
    )