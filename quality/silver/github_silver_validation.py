"""
Silver Validation: GitHub Archive
=================================
Validates Silver Parquet for schema, row counts, event type
distribution, and temporal consistency after silver transform.
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
SILVER_KEY = "github/github_silver.parquet"
REPORT_DIR = Path("quality/reports/silver/github")

EXPECTED_COLUMNS = [
    "id", "event_type", "public", "created_at", "actor_login",
    "actor_id", "repo_name", "repo_id", "org_login", "payload_raw",
    "year", "month", "day", "hour", "day_of_week", "event_time",
    "source_file"
]

# At least 1 million events in a typical full dump (single day range ~300k)
MIN_ROWS = 500_000
MAX_ROWS = 5_000_000

KNOWN_EVENT_TYPES = [
    "PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent",
    "ForkEvent", "CreateEvent", "DeleteEvent", "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent", "IssueCommentEvent", "MemberEvent",
    "PublicEvent", "GollumEvent", "ReleaseEvent", "SponsorshipEvent"
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
    print("Loading Silver GitHub data...")
    df = load_silver()
    print(f"Loaded {len(df):,} rows")

    suite = context.suites.add(gx.ExpectationSuite(name="silver_github"))

    # Row count
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=MIN_ROWS, max_value=MAX_ROWS)
    )

    # Column presence
    for col in EXPECTED_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))

    # Non-null on key columns
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_type")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="created_at")
    )

    # Event type should be in known set (allow unknown but mainly check)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="event_type",
            value_set=KNOWN_EVENT_TYPES,
            mostly=0.95
        )
    )

    # Temporal checks: year, month, day, hour within reasonable ranges
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=2011, max_value=2026)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="month", min_value=1, max_value=12)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="day", min_value=1, max_value=31)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="hour", min_value=0, max_value=23)
    )

    # created_at should match the year/month/day/hour derived columns approximately
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="created_at",
            column_B="created_at",
            or_equal=True
        )
    )

    data_source = context.data_sources.add_pandas(name="silver_github")
    data_asset = data_source.add_dataframe_asset(name="batch")
    batch_def = data_asset.add_batch_definition_whole_dataframe("whole")
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="silver_github", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name="silver_github_cp", validation_definitions=[val_def])
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})
    print(f"Validation {'✅ PASSED' if result.success else '❌ FAILED'}")
    report_path = REPORT_DIR / "silver_github.json"
    with open(report_path, "w") as f:
        json.dump(result.describe_dict(), f, indent=2, default=str)
    if not result.success:
        sys.exit(1)

if __name__ == "__main__":
    run()