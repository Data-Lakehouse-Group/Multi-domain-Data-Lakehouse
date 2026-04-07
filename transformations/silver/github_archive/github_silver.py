"""
Silver Layer Transformation: GitHub Archive (DuckDB Optimized)
==============================================================
Reads Bronze Delta table (raw GitHub events with JSON strings),
flattens JSON fields and extracts temporal features using DuckDB's JSON functions,
then writes clean Parquet to Silver.
"""

import os
import sys
import logging
import io
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
import polars as pl
import boto3
from botocore.client import Config
from deltalake import DeltaTable

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
BRONZE_BUCKET = os.getenv('BRONZE_BUCKET', 'bronze')
SILVER_BUCKET = os.getenv('SILVER_BUCKET', 'silver')

STORAGE_OPTIONS = {
    'AWS_ENDPOINT_URL': MINIO_ENDPOINT,
    'AWS_ACCESS_KEY_ID': MINIO_ACCESS_KEY,
    'AWS_SECRET_ACCESS_KEY': MINIO_SECRET_KEY,
    'AWS_ALLOW_HTTP': 'true',
}

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def read_delta_to_duckdb(uri: str, table_name: str, con: duckdb.DuckDBPyConnection):
    """Read Delta table into DuckDB as a temporary table."""
    log.info(f"Loading Delta table {uri} into DuckDB as {table_name}")
    dt = DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    pdf = dt.to_pyarrow_table().to_pandas()
    con.register(table_name, pdf)
    log.info(f"  → {len(pdf):,} rows loaded")

def run():
    log.info("=" * 70)
    log.info("GITHUB SILVER TRANSFORMATION (DuckDB Optimized)")
    log.info("=" * 70)

    con = duckdb.connect()

    # -----------------------------------------------------------------------
    # 1. Load Bronze GitHub Delta table
    # -----------------------------------------------------------------------
    read_delta_to_duckdb(
        f"s3://{BRONZE_BUCKET}/github_archive",
        "github_raw",
        con
    )

    # -----------------------------------------------------------------------
    # 2. Flatten JSON strings and extract features using DuckDB's JSON functions
    # -----------------------------------------------------------------------
    log.info("Flattening JSON fields and extracting temporal features...")
    result = con.execute("""
        SELECT
            id,
            type AS event_type,
            public,
            created_at,
            -- Extract from JSON strings
            CAST(actor->>'login' AS VARCHAR) AS actor_login,
            CAST(repo->>'name' AS VARCHAR) AS repo_name,
            CAST(org->>'login' AS VARCHAR) AS org_login,
            -- Parse timestamp
            CAST(created_at AS TIMESTAMP) AS event_time,
            -- Temporal features
            EXTRACT(YEAR FROM CAST(created_at AS TIMESTAMP)) AS year,
            EXTRACT(MONTH FROM CAST(created_at AS TIMESTAMP)) AS month,
            EXTRACT(DAY FROM CAST(created_at AS TIMESTAMP)) AS day,
            EXTRACT(HOUR FROM CAST(created_at AS TIMESTAMP)) AS hour,
            EXTRACT(DOW FROM CAST(created_at AS TIMESTAMP)) AS day_of_week
        FROM github_raw
        WHERE created_at IS NOT NULL
          AND type IS NOT NULL
    """)

    df_pandas = result.df()
    df = pl.from_pandas(df_pandas)
    log.info(f"Transformed rows: {len(df):,}")

    # -----------------------------------------------------------------------
    # 3. Write to Silver bucket as Parquet
    # -----------------------------------------------------------------------
    client = get_s3_client()
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    silver_key = "github/github_silver.parquet"
    client.put_object(Bucket=SILVER_BUCKET, Key=silver_key, Body=buf.getvalue())
    log.info(f"✅ Silver GitHub written to s3://{SILVER_BUCKET}/{silver_key}")

    con.close()
    log.info("✅ GITHUB SILVER COMPLETE")

if __name__ == "__main__":
    run()