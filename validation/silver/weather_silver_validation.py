"""
Silver Validation: NOAA Weather
===============================
Validates Silver Parquet for cleanliness, value ranges, and temporal consistency.
"""

import os
import io
import json
import sys
from pathlib import Path
import great_expectations as gx
import pandas as pd
import boto3
from botocore.client import Config

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_BUCKET = "silver"
SILVER_KEY = "weather/gsod_silver.parquet"
REPORT_DIR = Path("quality/reports/silver/weather")

EXPECTED_COLUMNS = [
    "STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME",
    "TEMP", "DEWP", "SLP", "STP", "VISIB", "WDSP", "MXSPD", "GUST",
    "MAX", "MIN", "PRCP", "SNDP", "FRSHTT",
    "year", "month", "day_of_week", "season", "is_freezing", "has_precipitation"
]

def get_s3_client():
    return boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"), region_name="us-east-1"
    )

def load_silver() -> pd.DataFrame:
    client = get_s3_client()
    resp = client.get_object(Bucket=SILVER_BUCKET, Key=SILVER_KEY)
    return pd.read_parquet(io.BytesIO(resp["Body"].read()))

def run():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    context = gx.get_context()
    print("Loading Silver weather data...")
    df = load_silver()
    print(f"Loaded {len(df):,} rows")

    suite = context.suites.add(gx.ExpectationSuite(name="silver_weather"))

    # Row count should be > 2 million for a couple years of global data
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=2_000_000, max_value=10_000_000)
    )

    # Column presence
    for col in EXPECTED_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))

    # Non-null critical columns
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="STATION")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="DATE")
    )

    # Temperature range (degrees Celsius, global extremes)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="TEMP", min_value=-90, max_value=60)
    )
    # Precipitation (mm) – no negative, max 2000 mm/day (extreme)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="PRCP", min_value=0, max_value=2000)
    )
    # Wind speed (m/s) – typical max ~50 m/s (hurricane force)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="WDSP", min_value=0, max_value=50)
    )

    # Season should be one of four values
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="season", value_set=["winter", "spring", "summer", "fall"])
    )

    # is_freezing should be 0 or 1
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="is_freezing", value_set=[0, 1])
    )

    # Date should be within reasonable range (e.g., 1900-2026)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="DATE",
            min_value=pd.Timestamp("1900-01-01"),
            max_value=pd.Timestamp("2026-12-31")
        )
    )

    data_source = context.data_sources.add_pandas(name="silver_weather")
    data_asset = data_source.add_dataframe_asset(name="batch")
    batch_def = data_asset.add_batch_definition_whole_dataframe("whole")
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="silver_weather", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name="silver_weather_cp", validation_definitions=[val_def])
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})
    print(f"Validation {'✅ PASSED' if result.success else '❌ FAILED'}")
    report_path = REPORT_DIR / "silver_weather.json"
    with open(report_path, "w") as f:
        json.dump(result.describe_dict(), f, indent=2, default=str)
    if not result.success:
        sys.exit(1)

if __name__ == "__main__":
    run()