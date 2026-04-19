"""
Gold Layer: NYC Taxi Analytics
==============================
Reads Silver taxi Parquet from MinIO and produces:
- daily_stats: trips, revenue, avg fare, tip % by day and zone
- hourly_heatmap: pickup counts by hour and zone
- top_routes: most frequent pickup/dropoff pairs

Uses DuckDB for high‑performance in‑memory aggregations.
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"
SILVER_KEY = "taxi/yellow_taxi_silver.parquet"

# Gold output keys
DAILY_STATS_KEY = "taxi/daily_stats.parquet"
HOURLY_HEATMAP_KEY = "taxi/hourly_heatmap.parquet"
TOP_ROUTES_KEY = "taxi/top_routes.parquet"

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
    """Read silver Parquet from MinIO into a DuckDB connection."""
    client = get_s3_client()
    resp = client.get_object(Bucket=SILVER_BUCKET, Key=SILVER_KEY)
    data = resp["Body"].read()
    con = duckdb.connect()
    # DuckDB can directly read Parquet from a bytes buffer
    con.execute("CREATE OR REPLACE TABLE silver AS SELECT * FROM read_parquet(?::BLOB)", [data])
    row_count = con.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
    log.info(f"Loaded silver taxi data: {row_count:,} rows")
    return con

def write_parquet_to_gold(df: pl.DataFrame, key: str):
    """Write Polars DataFrame to gold bucket as Parquet."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client = get_s3_client()
    client.put_object(Bucket=GOLD_BUCKET, Key=key, Body=buf.getvalue())
    log.info(f"Written gold table to s3://{GOLD_BUCKET}/{key} ({len(df):,} rows)")

def build_daily_stats(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        DATE_TRUNC('day', tpep_pickup_datetime) AS trip_date,
        PU_Borough,
        PU_Zone,
        COUNT(*) AS num_trips,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_fare,
        AVG(tip_percentage) AS avg_tip_pct,
        AVG(trip_distance) AS avg_distance
    FROM silver
    GROUP BY trip_date, PU_Borough, PU_Zone
    ORDER BY trip_date, PU_Borough, PU_Zone
    """
    return con.execute(query).pl()

def build_hourly_heatmap(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        pickup_hour,
        pickup_day_of_week,
        PU_Borough,
        PU_Zone,
        COUNT(*) AS pickup_count
    FROM silver
    GROUP BY pickup_hour, pickup_day_of_week, PU_Borough, PU_Zone
    ORDER BY pickup_hour, pickup_day_of_week, PU_Borough, PU_Zone
    """
    return con.execute(query).pl()

def build_top_routes(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        PU_Borough,
        PU_Zone,
        DO_Borough,
        DO_Zone,
        COUNT(*) AS trip_count,
        AVG(trip_distance) AS avg_distance,
        AVG(total_amount) AS avg_fare
    FROM silver
    GROUP BY PU_Borough, PU_Zone, DO_Borough, DO_Zone
    ORDER BY trip_count DESC
    LIMIT 1000
    """
    return con.execute(query).pl()

def run():
    log.info("=" * 70)
    log.info("TAXI GOLD TRANSFORMATION")
    log.info("=" * 70)

    con = read_silver_parquet()

    log.info("Building daily_stats...")
    daily_stats = build_daily_stats(con)
    write_parquet_to_gold(daily_stats, DAILY_STATS_KEY)

    log.info("Building hourly_heatmap...")
    hourly_heatmap = build_hourly_heatmap(con)
    write_parquet_to_gold(hourly_heatmap, HOURLY_HEATMAP_KEY)

    log.info("Building top_routes...")
    top_routes = build_top_routes(con)
    write_parquet_to_gold(top_routes, TOP_ROUTES_KEY)

    con.close()
    log.info("✅ TAXI GOLD COMPLETE")

if __name__ == "__main__":
    run()