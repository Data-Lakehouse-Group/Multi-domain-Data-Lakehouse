"""
Silver Validation: NYC Taxi
===========================
Validates Silver Delta table for schema, row counts, business rules,
and derived column integrity on a per-month basis.

Runs AFTER silver_transform.py.

Usage:
    python quality/taxi/silver_suite.py
    python quality/taxi/silver_suite.py --year 2023
    python quality/taxi/silver_suite.py --year 2023 --month-start 1 --month-end 1
"""

import os
import calendar
import argparse
import json
import sys
import boto3

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import CheckpointResult
import pandas as pd
from deltalake import DeltaTable

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SILVER_URI = "s3://silver/taxi/yellow_tripdata"
REPORT_URI = "s3://artifacts/great_expectations/reports/taxi/silver"

STORAGE_OPTIONS = {
    "endpoint_url"          : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"     : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key" : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"            : "true",
    "aws_region"            : os.getenv("AWS_REGION", "us-east-1"),
    "aws_s3_allow_unsafe_rename": "true",
}

EXPECTED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "trip_duration_minutes",
    "trip_speed_mph",
    "pickup_hour",
    "pickup_day_of_week",
    "day_name",
    "is_weekend",
    "is_shared_ride",
    "processed_at",
]

MIN_ROWS_PER_MONTH = 100_000
MAX_ROWS_PER_MONTH = 6_000_000

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def load_silver_month(year: int, month: int) -> pd.DataFrame:
    """Read one month partition from the Silver Delta table."""
    try:
        dt = DeltaTable(SILVER_URI, storage_options=STORAGE_OPTIONS)
        arrow_table = dt.to_pyarrow_table(
            filters=[
                ("source_year", "=", year),
                ("source_month", "=", month),
            ]
        )
        return arrow_table.to_pandas()
    except Exception as e:
        raise RuntimeError(
            f"No Silver data found for {year}-{month:02d}. "
            f"Ensure silver_transform.py succeeded for this month. Error: {e}"
        )

def save_validation_report(result: CheckpointResult, year: int, month: int):
    report_name = f"{year}_{month:02d}.json"
    report_content = json.dumps(result.describe_dict(), indent=2, default=str)

    s3_client = boto3.client(
        "s3",
        endpoint_url          = STORAGE_OPTIONS["endpoint_url"],
        aws_access_key_id     = STORAGE_OPTIONS["aws_access_key_id"],
        aws_secret_access_key = STORAGE_OPTIONS["aws_secret_access_key"],
        region_name           = STORAGE_OPTIONS["aws_region"],
    )
    s3_client.put_object(
        Bucket      = "artifacts",
        Key         = f"great_expectations/reports/taxi/silver/{report_name}",
        Body        = report_content.encode("utf-8"),
        ContentType = "application/json",
    )
    print(f"\n  Report saved : {REPORT_URI}/{report_name}")

def print_report(result: CheckpointResult, year: int, month: int):
    print(f"\n  {'=' * 55}")
    print(f"  SILVER VALIDATION — {calendar.month_name[month]} {year}")
    print(f"  {'=' * 55}")
    vr = list(result.run_results.values())[0]["results"]
    passed = sum(1 for r in vr if r["success"])
    total = len(vr)
    print(f"  Checks passed : {passed}/{total}")
    print(f"  Status        : {'✅ PASSED' if result.success else '❌ FAILED'}")
    if not result.success:
        print(f"\n  Failed checks:")
        for r in vr:
            if not r["success"]:
                exp_type = r["expectation_config"]["type"]
                col = r["expectation_config"]["kwargs"].get("column", "table")
                obs = r.get("result", {}).get("observed_value", "N/A")
                print(f"    ✗ {exp_type} | Column: {col} | Observed: {obs}")

# ---------------------------------------------------------------------------
# EXPECTATION SUITE
# ---------------------------------------------------------------------------

def build_silver_suite(context, suite_name: str) -> gx.ExpectationSuite:
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    # Row count
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=MIN_ROWS_PER_MONTH,
            max_value=MAX_ROWS_PER_MONTH,
        )
    )

    # Schema
    for col in EXPECTED_COLUMNS:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col)
        )

    # Non-null on critical columns
    for col in ["tpep_pickup_datetime", "fare_amount", "trip_distance", "processed_at"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col, mostly=0.99)
        )

    # Business rules
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="fare_amount", min_value=0.01, max_value=500
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="trip_distance", min_value=0.01, max_value=100
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="passenger_count", min_value=1, max_value=6
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="trip_duration_minutes", min_value=1, max_value=1440
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="tip_amount", min_value=0, max_value=1000
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="trip_speed_mph", min_value=0, max_value=120
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="is_weekend", value_set=[True, False]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="is_shared_ride", value_set=[True, False]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="payment_type",
            value_set=[1, 2, 3, 4, 5, 6],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pickup_hour", min_value=0, max_value=23
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pickup_day_of_week", min_value=0, max_value=6
        )
    )

    return suite

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run Silver validation suite for NYC Taxi data")
    parser.add_argument("--year", type=int, default=2023, help="Year to validate (default: 2023)")
    parser.add_argument("--month-start", type=int, default=None, help="First month in range")
    parser.add_argument("--month-end", type=int, default=None, help="Last month in range")
    args = parser.parse_args()

    if args.month_start and args.month_end:
        if args.month_start > args.month_end:
            print("ERROR: month-start must be <= month-end")
            sys.exit(1)
        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)

    year = args.year
    print(f"\nNYC Taxi Silver Validation Suite")
    print(f"Year: {year} | Months: {', '.join(calendar.month_name[m] for m in months)}")

    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="silver_taxi")
    data_asset = data_source.add_dataframe_asset(name="silver_monthly_batch")

    results = {"passed": [], "failed": []}

    for month in months:
        print(f"\nValidating {calendar.month_name[month]} {year}...")
        try:
            df = load_silver_month(year, month)
            print(f"✅ Loaded {len(df):,} rows")
        except RuntimeError as e:
            print(f"  [ERROR] {e}")
            results["failed"].append(f"{year}_{month:02d}")
            continue

        suite = build_silver_suite(context, f"silver_taxi_{year}_{month:02d}")
        batch_def = data_asset.add_batch_definition_whole_dataframe(
            f"silver_{year}_{month:02d}"
        )
        validation_def = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=f"silver_validation_{year}_{month:02d}",
                data=batch_def,
                suite=suite,
            )
        )
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name=f"silver_checkpoint_{year}_{month:02d}",
                validation_definitions=[validation_def],
                result_format={"result_format": "SUMMARY"},
            )
        )
        result = checkpoint.run(batch_parameters={"dataframe": df})
        print_report(result, year, month)
        save_validation_report(result, year, month)

        if result.success:
            results["passed"].append(f"{year}_{month:02d}")
        else:
            results["failed"].append(f"{year}_{month:02d}")

    print(f"\n{'=' * 55}")
    print(f"SILVER VALIDATION COMPLETE")
    print(f"{'=' * 55}")
    print(f"  Passed : {len(results['passed'])} months")
    print(f"  Failed : {len(results['failed'])} months")
    if results["failed"]:
        print(f"  Failed : {', '.join(results['failed'])}")
        print(f"\n  ❌ Pipeline blocked. Fix Silver issues before running Gold.")
        sys.exit(1)
    else:
        print(f"\n  ✅ All checks passed. Safe to proceed to Gold.")

if __name__ == "__main__":
    main()