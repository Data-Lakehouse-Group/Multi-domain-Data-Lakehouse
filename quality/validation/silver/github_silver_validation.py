import os, io, json, sys
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

EXPECTED_COLUMNS = ["id", "event_type", "public", "created_at", "actor_login", "repo_name",
                    "org_login", "event_time", "year", "month", "day", "hour", "day_of_week"]

def get_s3_client():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                        aws_access_key_id=MINIO_ACCESS_KEY,
                        aws_secret_access_key=MINIO_SECRET_KEY,
                        config=Config(signature_version="s3v4"), region_name="us-east-1")

def load_silver() -> pd.DataFrame:
    client = get_s3_client()
    resp = client.get_object(Bucket=SILVER_BUCKET, Key=SILVER_KEY)
    return pd.read_parquet(io.BytesIO(resp["Body"].read()))

def run():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    context = gx.get_context()
    df = load_silver()
    print(f"Loaded {len(df):,} rows")

    suite = context.suites.add(gx.ExpectationSuite(name="silver_github"))
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1_000_000, max_value=10_000_000))
    for col in EXPECTED_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="event_type"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="repo_name"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="event_type",
        value_set=["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent", "ForkEvent", "CreateEvent"]))

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