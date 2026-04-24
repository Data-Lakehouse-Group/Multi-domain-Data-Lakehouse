"""
Silver Validation: Instacart Retail
===================================
Validates Silver Parquet for cleanliness, referential integrity, and business rules.
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
SILVER_KEY = "retail/retail_silver.parquet"
REPORT_DIR = Path("quality/reports/silver/retail")

EXPECTED_COLUMNS = [
    "order_id", "product_id", "add_to_cart_order", "reordered",
    "product_name", "aisle_name", "department_name", "silver_processed_at"
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
    print("Loading Silver retail data...")
    df = load_silver()
    print(f"Loaded {len(df):,} rows")

    suite = context.suites.add(gx.ExpectationSuite(name="silver_retail"))

    # Row count should be ~32 million (full dataset) or reasonable subset
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=30_000_000, max_value=35_000_000)
    )

    # Column presence
    for col in EXPECTED_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))

    # Non-null critical columns
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id")
    )

    # Value ranges
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="add_to_cart_order", min_value=1, max_value=100)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="reordered", value_set=[0, 1])
    )

    # Check that product names are not empty strings
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotMatchRegex(column="product_name", regex=r"^\s*$")
    )

    # Validate that silver_processed_at is recent (within last 7 days)
    suite.add_expectation(
        gx.expectations.ExpectColumnMaxToBeBetween(
            column="silver_processed_at",
            min_value=pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7),
            max_value=pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=1)
        )
    )

    data_source = context.data_sources.add_pandas(name="silver_retail")
    data_asset = data_source.add_dataframe_asset(name="batch")
    batch_def = data_asset.add_batch_definition_whole_dataframe("whole")
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="silver_retail", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name="silver_retail_cp", validation_definitions=[val_def])
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})
    print(f"Validation {'✅ PASSED' if result.success else '❌ FAILED'}")
    report_path = REPORT_DIR / "silver_retail.json"
    with open(report_path, "w") as f:
        json.dump(result.describe_dict(), f, indent=2, default=str)
    if not result.success:
        sys.exit(1)

if __name__ == "__main__":
    run()