"""Silver: clean, feature engineer, join zones, output to MinIO as Parquet"""
import os
import duckdb
import polars as pl

MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

BRONZE_URI = "s3://bronze/taxi/yellow_tripdata"
ZONES_URI = "s3://bronze/taxi/taxi_zone_lookup.csv"   # you need to upload this file to MinIO bronze/taxi/
SILVER_PATH = "s3://silver/taxi/yellow_taxi_silver.parquet"

os.environ["AWS_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["AWS_ACCESS_KEY_ID"] = ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = SECRET_KEY
os.environ["AWS_ALLOW_HTTP"] = "true"

def run():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    con.execute(f"CREATE VIEW bronze AS SELECT * FROM delta_scan('{BRONZE_URI}')")
    # Load zones (assume CSV already in bronze bucket)
    con.execute(f"CREATE VIEW zones AS SELECT * FROM read_csv('{ZONES_URI}', auto_detect=true)")

    silver_df = con.execute("""
        SELECT
            b.tpep_pickup_datetime,
            b.tpep_dropoff_datetime,
            b.PULocationID,
            b.DOLocationID,
            b.passenger_count,
            b.trip_distance,
            b.fare_amount,
            b.tip_amount,
            b.total_amount,
            b.payment_type,
            -- Zone names (human readable)
            pu.Borough AS PU_Borough,
            pu.Zone AS PU_Zone,
            do.Borough AS DO_Borough,
            do.Zone AS DO_Zone,
            -- Derived columns
            (EXTRACT(EPOCH FROM (b.tpep_dropoff_datetime - b.tpep_pickup_datetime)) / 60) AS trip_duration_minutes,
            EXTRACT(HOUR FROM b.tpep_pickup_datetime) AS pickup_hour,
            EXTRACT(DOW FROM b.tpep_pickup_datetime) AS pickup_day_of_week_num,
            TO_CHAR(b.tpep_pickup_datetime, 'Day') AS pickup_day_of_week,
            -- Speed (mph)
            CASE WHEN (EXTRACT(EPOCH FROM (b.tpep_dropoff_datetime - b.tpep_pickup_datetime)) / 3600) > 0
                 THEN b.trip_distance / (EXTRACT(EPOCH FROM (b.tpep_dropoff_datetime - b.tpep_pickup_datetime)) / 3600)
                 ELSE NULL END AS trip_speed_mph,
            -- Tip percentage (only credit card)
            CASE WHEN b.payment_type = 1 AND b.fare_amount > 0
                 THEN (b.tip_amount / b.fare_amount) * 100
                 ELSE NULL END AS tip_percentage
        FROM bronze b
        LEFT JOIN zones pu ON b.PULocationID = pu.LocationID
        LEFT JOIN zones do ON b.DOLocationID = do.LocationID
        WHERE b.tpep_pickup_datetime IS NOT NULL
          AND b.tpep_dropoff_datetime IS NOT NULL
          AND b.PULocationID IS NOT NULL
          AND b.DOLocationID IS NOT NULL
          AND b.fare_amount > 0 AND b.fare_amount <= 500
          AND b.trip_distance > 0
          AND b.tpep_dropoff_datetime > b.tpep_pickup_datetime
          AND (EXTRACT(EPOCH FROM (b.tpep_dropoff_datetime - b.tpep_pickup_datetime)) / 3600) > 0
    """).pl()

    # Remove extreme speeds (>150 mph)
    silver_df = silver_df.filter(pl.col("trip_speed_mph").is_null() | (pl.col("trip_speed_mph") <= 150))

    # Write to Silver bucket
    silver_df.write_parquet(SILVER_PATH, storage_options={
        "endpoint_url": MINIO_ENDPOINT,
        "aws_access_key_id": ACCESS_KEY,
        "aws_secret_access_key": SECRET_KEY,
        "allow_http": "true"
    })
    print(f"Silver saved: {len(silver_df):,} rows → {SILVER_PATH}")

if __name__ == "__main__":
    run()