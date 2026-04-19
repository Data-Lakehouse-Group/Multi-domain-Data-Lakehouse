"""
Gold Layer: GitHub Archive Analytics
====================================
Reads Silver GitHub Parquet and produces:
- daily_activity: event counts by type, repo, and day
- top_repos: most active repositories
- hourly_patterns: event volume by hour and day of week
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
SILVER_KEY = "github/github_silver.parquet"

DAILY_ACTIVITY_KEY = "github/daily_activity.parquet"
TOP_REPOS_KEY = "github/top_repos.parquet"
HOURLY_PATTERNS_KEY = "github/hourly_patterns.parquet"

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
    log.info(f"Loaded silver GitHub data: {row_count:,} rows")
    return con

def write_parquet_to_gold(df: pl.DataFrame, key: str):
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client = get_s3_client()
    client.put_object(Bucket=GOLD_BUCKET, Key=key, Body=buf.getvalue())
    log.info(f"Written gold table to s3://{GOLD_BUCKET}/{key} ({len(df):,} rows)")

def build_daily_activity(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        DATE_TRUNC('day', event_time) AS event_date,
        event_type,
        repo_name,
        COUNT(*) AS event_count
    FROM silver
    GROUP BY event_date, event_type, repo_name
    ORDER BY event_date, event_type, repo_name
    """
    return con.execute(query).pl()

def build_top_repos(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        repo_name,
        COUNT(*) AS total_events,
        COUNT(DISTINCT actor_login) AS unique_actors,
        SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) AS push_events,
        SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pr_events,
        SUM(CASE WHEN event_type = 'IssuesEvent' THEN 1 ELSE 0 END) AS issues_events
    FROM silver
    GROUP BY repo_name
    ORDER BY total_events DESC
    LIMIT 100
    """
    return con.execute(query).pl()

def build_hourly_patterns(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        hour,
        day_of_week,
        COUNT(*) AS event_count
    FROM silver
    GROUP BY hour, day_of_week
    ORDER BY day_of_week, hour
    """
    return con.execute(query).pl()

def run():
    log.info("=" * 70)
    log.info("GITHUB GOLD TRANSFORMATION")
    log.info("=" * 70)

    con = read_silver_parquet()

    log.info("Building daily_activity...")
    daily_activity = build_daily_activity(con)
    write_parquet_to_gold(daily_activity, DAILY_ACTIVITY_KEY)

    log.info("Building top_repos...")
    top_repos = build_top_repos(con)
    write_parquet_to_gold(top_repos, TOP_REPOS_KEY)

    log.info("Building hourly_patterns...")
    hourly_patterns = build_hourly_patterns(con)
    write_parquet_to_gold(hourly_patterns, HOURLY_PATTERNS_KEY)

    con.close()
    log.info("✅ GITHUB GOLD COMPLETE")

if __name__ == "__main__":
    run()