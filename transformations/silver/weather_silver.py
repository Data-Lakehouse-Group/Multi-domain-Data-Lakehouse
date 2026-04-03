"""Silver Layer: Weather - reads full Bronze Delta"""
import os
import logging
from deltalake import DeltaTable
import polars as pl
import boto3
from botocore.client import Config

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
BRONZE_WEATHER_PATH = "s3://bronze/weather"
SILVER_WEATHER_KEY = "weather/gsod_silver.parquet"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def read_bronze_weather():
    storage_opts = {
        "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        "AWS_ALLOW_HTTP": "true",
    }
    dt = DeltaTable(BRONZE_WEATHER_PATH, storage_options=storage_opts)
    df = pl.from_arrow(dt.to_pyarrow_table())
    log.info(f"Read {len(df)} rows from Bronze weather")
    return df

def clean(df):
    initial = len(df)
    # Replace missing codes (9999 etc.)
    df = df.with_columns([
        pl.when(pl.col("TEMP") == 9999).then(None).otherwise(pl.col("TEMP")).alias("TEMP"),
        pl.when(pl.col("PRCP") == 99999).then(None).otherwise(pl.col("PRCP")).alias("PRCP"),
        pl.when(pl.col("WDSP") == 999999).then(None).otherwise(pl.col("WDSP")).alias("WDSP"),
    ])
    df = df.drop_nulls(subset=["TEMP", "PRCP", "WDSP"])
    df = df.filter((pl.col("TEMP") >= -60) & (pl.col("TEMP") <= 60))
    df = df.filter(pl.col("PRCP") <= 400)
    df = df.filter(pl.col("WDSP") <= 50)
    log.info(f"Cleaned: {initial} -> {len(df)} rows")
    return df

def engineer_features(df):
    df = df.with_columns([
        pl.col("DATE").cast(pl.Date).dt.year().alias("year"),
        pl.col("DATE").cast(pl.Date).dt.month().alias("month"),
        pl.col("DATE").cast(pl.Date).dt.weekday().alias("day_of_week"),
        pl.when(pl.col("DATE").cast(pl.Date).dt.month().is_in([12,1,2])).then(pl.lit("winter"))
          .when(pl.col("DATE").cast(pl.Date).dt.month().is_in([3,4,5])).then(pl.lit("spring"))
          .when(pl.col("DATE").cast(pl.Date).dt.month().is_in([6,7,8])).then(pl.lit("summer"))
          .otherwise(pl.lit("fall")).alias("season"),
        pl.when(pl.col("TEMP") <= 0).then(1).otherwise(0).alias("is_freezing"),
        pl.when(pl.col("PRCP") > 0).then(1).otherwise(0).alias("has_precipitation"),
    ])
    return df

def write_silver(df):
    import boto3, io
    client = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY,
                          config=Config(signature_version="s3v4"))
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client.put_object(Bucket=SILVER_BUCKET, Key=SILVER_WEATHER_KEY, Body=buf.getvalue())
    log.info(f"Silver weather written to s3://{SILVER_BUCKET}/{SILVER_WEATHER_KEY}")

def run():
    df = read_bronze_weather()
    df = clean(df)
    df = engineer_features(df)
    write_silver(df)

if __name__ == "__main__":
    run()