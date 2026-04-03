"""Silver Layer: GitHub Archive - flatten JSON payloads"""
import os, logging, json, io
import polars as pl
import boto3
from botocore.client import Config
from deltalake import DeltaTable

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

storage_opts = {
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ALLOW_HTTP": "true",
}

def run():
    dt = DeltaTable("s3://bronze/github_archive", storage_options=storage_opts)
    df = pl.from_arrow(dt.to_pyarrow_table())

    # Extract basic fields from JSON strings
    df = df.with_columns([
        pl.col("actor").map_elements(lambda x: json.loads(x).get("login", None), return_dtype=pl.String).alias("actor_login"),
        pl.col("repo").map_elements(lambda x: json.loads(x).get("name", None), return_dtype=pl.String).alias("repo_name"),
        pl.col("type").alias("event_type"),
        pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ").alias("event_time"),
    ])

    # Add temporal columns
    df = df.with_columns([
        df["event_time"].dt.year().alias("year"),
        df["event_time"].dt.month().alias("month"),
        df["event_time"].dt.hour().alias("hour"),
    ])

    # Select clean columns
    df_clean = df.select(["id", "event_type", "actor_login", "repo_name", "public", "event_time", "year", "month", "hour"])

    # Write to silver
    client = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY,
                          config=Config(signature_version="s3v4"))
    buf = io.BytesIO()
    df_clean.write_parquet(buf)
    buf.seek(0)
    client.put_object(Bucket=SILVER_BUCKET, Key="github/github_silver.parquet", Body=buf.getvalue())
    logging.info("Silver GitHub written")

if __name__ == "__main__":
    run()