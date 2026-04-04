"""
Silver Layer Transformation: Instacart Retail Data (DuckDB Optimized)
=====================================================================
Reads Bronze Delta tables (order_products) and CSV lookup tables,
joins them using DuckDB for fast execution, and writes cleaned Parquet to Silver.
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
    """Read a Delta table into DuckDB as a temporary view."""
    log.info(f"Loading Delta table {uri} into DuckDB as {table_name}")
    dt = DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    # Convert to Pandas (DuckDB can read Pandas directly)
    pdf = dt.to_pyarrow_table().to_pandas()
    con.register(table_name, pdf)
    log.info(f"  → {len(pdf):,} rows loaded")

def read_csv_from_minio(key: str, con: duckdb.DuckDBPyConnection, table_name: str):
    """Read a CSV from MinIO into DuckDB as a temporary view."""
    log.info(f"Loading CSV s3://{BRONZE_BUCKET}/{key} as {table_name}")
    client = get_s3_client()
    resp = client.get_object(Bucket=BRONZE_BUCKET, Key=key)
    csv_data = resp['Body'].read()
    # DuckDB can read directly from bytes (file-like)
    import pandas as pd
    df = pd.read_csv(io.BytesIO(csv_data))
    con.register(table_name, df)
    log.info(f"  → {len(df):,} rows loaded")

def run():
    log.info("=" * 70)
    log.info("RETAIL SILVER TRANSFORMATION (DuckDB Optimized)")
    log.info("=" * 70)

    # Create DuckDB connection
    con = duckdb.connect()

    # -----------------------------------------------------------------------
    # 1. Load all Bronze tables into DuckDB
    # -----------------------------------------------------------------------
    # Order products (Delta table) - the large fact table
    read_delta_to_duckdb(
        f"s3://{BRONZE_BUCKET}/retail/order_products",
        "order_products",
        con
    )

    # Lookup tables (CSV files stored in Bronze bucket)
    read_csv_from_minio("retail/products.csv", con, "products")
    read_csv_from_minio("retail/aisles.csv", con, "aisles")
    read_csv_from_minio("retail/departments.csv", con, "departments")

    # -----------------------------------------------------------------------
    # 2. Perform joins using DuckDB (very fast, columnar)
    # -----------------------------------------------------------------------
    log.info("Performing joins...")
    result = con.execute("""
        SELECT
            op.order_id,
            op.product_id,
            op.add_to_cart_order,
            op.reordered,
            p.product_name,
            a.aisle AS aisle_name,
            d.department AS department_name,
            -- Add audit info
            CURRENT_TIMESTAMP AS silver_processed_at
        FROM order_products op
        LEFT JOIN products p ON op.product_id = p.product_id
        LEFT JOIN aisles a ON p.aisle_id = a.aisle_id
        LEFT JOIN departments d ON p.department_id = d.department_id
        WHERE op.product_id IS NOT NULL
    """)

    # Fetch as Pandas then convert to Polars (or keep as Pandas for Parquet)
    df_pandas = result.df()
    df = pl.from_pandas(df_pandas)
    log.info(f"Joined rows: {len(df):,}")

    # -----------------------------------------------------------------------
    # 3. Write to Silver bucket as Parquet
    # -----------------------------------------------------------------------
    client = get_s3_client()
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    silver_key = "retail/retail_silver.parquet"
    client.put_object(Bucket=SILVER_BUCKET, Key=silver_key, Body=buf.getvalue())
    log.info(f"✅ Silver retail written to s3://{SILVER_BUCKET}/{silver_key}")

    # Close DuckDB connection
    con.close()
    log.info("✅ RETAIL SILVER COMPLETE")

if __name__ == "__main__":
    run()