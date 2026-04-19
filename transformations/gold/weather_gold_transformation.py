"""
Gold Layer: NOAA Weather Analytics
==================================
Reads Silver weather Parquet and produces:
- station_summary: yearly stats per station (avg temp, precip days, etc.)
- monthly_climate: monthly aggregates by station
- extreme_events: days with extreme temperature or precipitation
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
SILVER_KEY = "weather/gsod_silver.parquet"

STATION_SUMMARY_KEY = "weather/station_summary.parquet"
MONTHLY_CLIMATE_KEY = "weather/monthly_climate.parquet"
EXTREME_EVENTS_KEY = "weather/extreme_events.parquet"

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
    log.info(f"Loaded silver weather data: {row_count:,} rows")
    return con

def write_parquet_to_gold(df: pl.DataFrame, key: str):
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client = get_s3_client()
    client.put_object(Bucket=GOLD_BUCKET, Key=key, Body=buf.getvalue())
    log.info(f"Written gold table to s3://{GOLD_BUCKET}/{key} ({len(df):,} rows)")

def build_station_summary(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        STATION,
        year,
        COUNT(*) AS days_recorded,
        AVG(TEMP) AS avg_temp,
        MIN(TEMP) AS min_temp,
        MAX(TEMP) AS max_temp,
        SUM(PRCP) AS total_precip,
        AVG(WDSP) AS avg_wind_speed,
        SUM(has_precipitation) AS precip_days
    FROM silver
    GROUP BY STATION, year
    ORDER BY STATION, year
    """
    return con.execute(query).pl()

def build_monthly_climate(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        STATION,
        year,
        month,
        AVG(TEMP) AS avg_temp,
        SUM(PRCP) AS total_precip,
        AVG(WDSP) AS avg_wind_speed,
        COUNT(*) AS days_in_month
    FROM silver
    GROUP BY STATION, year, month
    ORDER BY STATION, year, month
    """
    return con.execute(query).pl()

def build_extreme_events(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    query = """
    SELECT
        STATION,
        DATE,
        TEMP,
        PRCP,
        WDSP,
        CASE
            WHEN TEMP > 35 THEN 'extreme_heat'
            WHEN TEMP < -20 THEN 'extreme_cold'
            WHEN PRCP > 50 THEN 'heavy_rain'
            WHEN WDSP > 20 THEN 'high_wind'
        END AS event_type
    FROM silver
    WHERE TEMP > 35 OR TEMP < -20 OR PRCP > 50 OR WDSP > 20
    ORDER BY DATE, STATION
    """
    return con.execute(query).pl()

def run():
    log.info("=" * 70)
    log.info("WEATHER GOLD TRANSFORMATION")
    log.info("=" * 70)

    con = read_silver_parquet()

    log.info("Building station_summary...")
    station_summary = build_station_summary(con)
    write_parquet_to_gold(station_summary, STATION_SUMMARY_KEY)

    log.info("Building monthly_climate...")
    monthly_climate = build_monthly_climate(con)
    write_parquet_to_gold(monthly_climate, MONTHLY_CLIMATE_KEY)

    log.info("Building extreme_events...")
    extreme_events = build_extreme_events(con)
    write_parquet_to_gold(extreme_events, EXTREME_EVENTS_KEY)

    con.close()
    log.info("✅ WEATHER GOLD COMPLETE")

if __name__ == "__main__":
    run()