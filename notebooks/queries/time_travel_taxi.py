"""
Time Travel Query Examples – NYC Taxi Bronze
============================================
Shows how to use Delta Lake time travel to inspect historical
versions of the Bronze taxi table, compare data across ingestion
runs, and roll back if needed.

Usage:
    python queries/time_travel_taxi.py
    python queries/time_travel_taxi.py --year 2023 --month 1
"""

import os
import argparse
import calendar
import duckdb
import pyarrow as pa
from deltalake import DeltaTable

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BRONZE_URI = "s3://bronze/taxi/yellow_tripdata"
STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : os.getenv("AWS_REGION", "us-east-1"),
    "aws_s3_allow_unsafe_rename": "true",
}

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def get_table_versions(uri):
    """List all available Delta table versions."""
    dt = DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    print(f"\nDelta table at {uri}")
    print(f"Current version: {dt.version()}")
    print(f"Last 10 versions:")
    history = dt.history(10)
    for entry in history:
        print(f"  Version {entry['version']} | Timestamp: {entry['timestamp']} | "
              f"Operation: {entry['operation']} | rows: {entry.get('operationMetrics', {}).get('numOutputRows', 'N/A')}")

def query_as_of_version(uri, version, filters):
    """Read data as of a specific version."""
    dt = DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    # Load an older version
    old_dt = dt.load_as_version(version)
    table = old_dt.to_pyarrow_table(filters=filters)
    return table

def query_as_of_timestamp(uri, timestamp_str, filters):
    """Read data as of a timestamp (ISO format)."""
    dt = DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    old_dt = dt.load_with_datetime(timestamp_str)
    table = old_dt.to_pyarrow_table(filters=filters)
    return table

def compare_versions(uri, version_a, version_b, year, month):
    """Compare two versions for the same month partition."""
    filters = [("source_year", "=", year), ("source_month", "=", month)]
    table_a = query_as_of_version(uri, version_a, filters)
    table_b = query_as_of_version(uri, version_b, filters)
    print(f"\nComparing version {version_a} ({table_a.num_rows} rows) and {version_b} ({table_b.num_rows} rows):")
    print(f"Rows added in v{version_b}: {table_b.num_rows - table_a.num_rows}")

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Demonstrate time travel on Taxi Bronze")
    parser.add_argument("--year",  type=int, default=2023, help="Year to filter")
    parser.add_argument("--month", type=int, default=1,   help="Month to filter")
    args = parser.parse_args()

    get_table_versions(BRONZE_URI)

    # Example: compare the latest two versions (if at least two exist)
    dt = DeltaTable(BRONZE_URI, storage_options=STORAGE_OPTIONS)
    if dt.version() >= 1:
        compare_versions(BRONZE_URI, dt.version() - 1, dt.version(), args.year, args.month)

    # Example: query as of a specific timestamp (adjust or set from args)
    # timestamp = "2023-03-01T12:00:00Z"
    # table = query_as_of_timestamp(BRONZE_URI, timestamp,
    #                               [("source_year", "=", args.year),
    #                                ("source_month", "=", args.month)])
    # print(f"As of {timestamp}, rows: {table.num_rows}")

if __name__ == "__main__":
    main()