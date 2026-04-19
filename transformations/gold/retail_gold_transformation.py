"""
Gold Layer: Instacart Retail Analytics
======================================
Reads Silver retail Parquet and produces:
- product_affinity: frequently bought together product pairs
- reorder_behavior: reorder rates by department and aisle
- department_performance: top departments by order volume
"""

import os
import io
import logging
import boto3
import duckdb
import polars as pl
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"
SILVER_KEY = "retail/retail_silver.parquet"

PRODUCT_AFFINITY_KEY = "retail/product_affinity.parquet"
REORDER_BEHAVIOR_KEY = "retail/reorder_behavior.parquet"
DEPT_PERFORMANCE_KEY = "retail/department_performance.parquet"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def read_silver_parquet():
    client = get_s3_client()
    resp = client.get_object(Bucket=SILVER_BUCKET, Key=SILVER_KEY)
    data = resp["Body"].read()
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE TABLE silver AS SELECT * FROM read_parquet(?::BLOB)", [data])
    row_count = con.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
    log.info(f"Loaded silver retail data: {row_count:,} rows")
    return con

def write_parquet_to_gold(df: pl.DataFrame, key: str):
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client = get_s3_client()
    client.put_object(Bucket=GOLD_BUCKET, Key=key, Body=buf.getvalue())
    log.info(f"Written gold table to s3://{GOLD_BUCKET}/{key} ({len(df):,} rows)")

def build_product_affinity(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    WITH order_pairs AS (
        SELECT
            a.order_id,
            a.product_id AS product_a,
            b.product_id AS product_b
        FROM silver a
        JOIN silver b ON a.order_id = b.order_id AND a.product_id < b.product_id
    )
    SELECT
        product_a,
        product_b,
        COUNT(*) AS co_occurrence_count
    FROM order_pairs
    GROUP BY product_a, product_b
    ORDER BY co_occurrence_count DESC
    LIMIT 1000
    """
    return con.execute(query).pl()

def build_reorder_behavior(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        department_name,
        aisle_name,
        AVG(reordered) AS reorder_rate,
        COUNT(*) AS total_orders,
        COUNT(DISTINCT product_id) AS distinct_products
    FROM silver
    GROUP BY department_name, aisle_name
    ORDER BY reorder_rate DESC
    """
    return con.execute(query).pl()

def build_department_performance(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        department_name,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(*) AS item_count,
        AVG(add_to_cart_order) AS avg_cart_position
    FROM silver
    GROUP BY department_name
    ORDER BY item_count DESC
    """
    return con.execute(query).pl()

def run():
    log.info("=" * 70)
    log.info("RETAIL GOLD TRANSFORMATION")
    log.info("=" * 70)

    con = read_silver_parquet()

    log.info("Building product_affinity...")
    product_affinity = build_product_affinity(con)
    write_parquet_to_gold(product_affinity, PRODUCT_AFFINITY_KEY)

    log.info("Building reorder_behavior...")
    reorder_behavior = build_reorder_behavior(con)
    write_parquet_to_gold(reorder_behavior, REORDER_BEHAVIOR_KEY)

    log.info("Building department_performance...")
    dept_perf = build_department_performance(con)
    write_parquet_to_gold(dept_perf, DEPT_PERFORMANCE_KEY)

    con.close()
    log.info("✅ RETAIL GOLD COMPLETE")

if __name__ == "__main__":
    run()