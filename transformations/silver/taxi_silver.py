"""Silver Layer: NYC Taxi - reads full Bronze Delta table"""
import os
import logging
from deltalake import DeltaTable
import polars as pl
import boto3
from botocore.client import Config

# Config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
BRONZE_TAXI_PATH = "s3://bronze/taxi/yellow_tripdata"
BRONZE_ZONES_KEY = "taxi/taxi_zone_lookup.csv"
SILVER_TAXI_KEY = "taxi/yellow_taxi_silver.parquet"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def get_s3_client():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY,
                        config=Config(signature_version="s3v4"),
                        region_name="us-east-1")

def read_bronze_taxi():
    """Read entire Bronze Delta table using DeltaTable + PyArrow"""
    storage_opts = {
        "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        "AWS_ALLOW_HTTP": "true",
    }
    dt = DeltaTable(BRONZE_TAXI_PATH, storage_options=storage_opts)
    # Convert to Polars
    df = pl.from_arrow(dt.to_pyarrow_table())
    log.info(f"Read {len(df)} rows from Bronze Delta table")
    return df

def read_zones():
    client = get_s3_client()
    resp = client.get_object(Bucket=BRONZE_BUCKET, Key=BRONZE_ZONES_KEY)
    import io
    return pl.read_csv(io.BytesIO(resp['Body'].read()))

def clean(df):
    initial = len(df)
    critical = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "fare_amount", "trip_distance"]
    df = df.drop_nulls(subset=critical)
    df = df.filter((pl.col("fare_amount") > 0) & (pl.col("fare_amount") <= 500))
    df = df.filter(pl.col("trip_distance") > 0)
    df = df.filter(pl.col("tpep_dropoff_datetime") > pl.col("tpep_pickup_datetime"))
    log.info(f"Cleaned: {initial} -> {len(df)} rows ({(len(df)/initial)*100:.1f}% kept)")
    return df

def engineer_features(df):
    df = df.with_columns([
        ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).dt.total_seconds() / 60).alias("trip_duration_minutes"),
        pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
        pl.col("tpep_pickup_datetime").dt.weekday().alias("pickup_day_of_week_num"),
        pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("pickup_day_of_week"),
    ])
    df = df.with_columns(
        pl.when(pl.col("trip_duration_minutes") > 0.5)
          .then(pl.col("trip_distance") / (pl.col("trip_duration_minutes") / 60))
          .otherwise(None).alias("trip_speed_mph")
    )
    df = df.filter((pl.col("trip_speed_mph").is_null()) | (pl.col("trip_speed_mph") <= 150))
    df = df.with_columns(
        pl.when((pl.col("payment_type") == 1) & (pl.col("fare_amount") > 0))
          .then(pl.col("tip_amount") / pl.col("fare_amount") * 100)
          .otherwise(None).alias("tip_percentage")
    )
    return df

def join_zones(df, zones):
    zones_pu = zones.rename({"LocationID": "PULocationID", "Borough": "PU_Borough", "Zone": "PU_Zone"})
    zones_do = zones.rename({"LocationID": "DOLocationID", "Borough": "DO_Borough", "Zone": "DO_Zone"})
    df = df.join(zones_pu.select(["PULocationID","PU_Borough","PU_Zone"]), on="PULocationID", how="left")
    df = df.join(zones_do.select(["DOLocationID","DO_Borough","DO_Zone"]), on="DOLocationID", how="left")
    return df

def write_silver(df):
    client = get_s3_client()
    import io
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client.put_object(Bucket=SILVER_BUCKET, Key=SILVER_TAXI_KEY, Body=buf.getvalue())
    log.info(f"Silver taxi written to s3://{SILVER_BUCKET}/{SILVER_TAXI_KEY}")

def run():
    df_bronze = read_bronze_taxi()
    zones = read_zones()
    df_clean = clean(df_bronze)
    df_features = engineer_features(df_clean)
    df_joined = join_zones(df_features, zones)
    write_silver(df_joined)

if __name__ == "__main__":
    run()