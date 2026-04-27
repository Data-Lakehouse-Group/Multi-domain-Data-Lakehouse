"""
NYC Taxi Silver Validation Suite
==================================
Validates that Silver transformation completed successfully through the following:
    - The schema matches our expected silver schema with all derived columns
    - The rows are between expected bounds per month
    - Critical columns have non-null values above a given threshold
    - Value ranges are physically plausible
    - Categorical/derived columns contain only valid values
    - Cross-field consistency between raw and derived columns

Runs AFTER silver_transform.py, BEFORE gold layer or dbt models

Usage:
    python quality/taxi/silver_suite.py
    python quality/taxi/silver_suite.py --year 2023
    python quality/taxi/silver_suite.py --year 2023 --month-start 1 --month-end 1
    python quality/taxi/silver_suite.py --year 2023 --month-start 1 --month-end 6
"""

import os
import calendar
import argparse
import boto3
import json
import sys

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.data_context import AbstractDataContext
import pandas as pd

from deltalake import DeltaTable


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SILVER_URI = "s3://silver/taxi"

REPORT_URI = "s3://artifacts/great_expectations/reports/taxi/silver"

STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : "us-east-1",
    "aws_s3_allow_unsafe_rename": "true",
}

# Expected silver schema — all necessary, derived, and audit columns
EXPECTED_COLUMNS = [
    # Original columns (optimised types)
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",

    # Bronze audit columns (passed through)
    "source_year",
    "source_month",
    "source_file",
    "ingested_at",

    # Derived columns
    "trip_duration_minutes",
    "pickup_day_of_week",
    "trip_speed_mph",
    "pickup_hour",
    "payment_method",
    "day_name",
    "is_weekend",
    "pickup_date",

    # Silver audit column
    "processed_at",
]

# Row count bounds — silver will always be <= bronze due to cleaning
MIN_ROWS_PER_MONTH = 80_000
MAX_ROWS_PER_MONTH = 6_000_000

# Null thresholds
EXPECTED_NON_NULL_PERCENTAGE        = 1  # critical columns
EXPECTED_NON_NULL_PERCENTAGE_SOFT   = 0.75  # optional/sparse columns

COLUMNS_CRITICAL_NOT_NULL = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "trip_distance",
    "passenger_count",
    "payment_type",
    "trip_duration_minutes",
    "pickup_day_of_week",
    "pickup_hour",
    "payment_method",
    "day_name",
    "is_weekend",
    "pickup_date",
    "source_year",
    "source_month",
    "processed_at",
]

COLUMNS_SOFT_NOT_NULL = [
    "trip_speed_mph",     # null when duration is zero
]

# Validation thresholds — must match silver_transform.py constants
MIN_FARE          = 0.0
MAX_FARE          = 500.0
MIN_TRIP_DISTANCE = 0.0
MIN_PASSENGERS    = 1
MAX_PASSENGERS    = 6

VALID_PAYMENT_METHODS = [
    "Credit Card",
    "Cash",
    "No Charge",
    "Dispute",
    "Unknown",
    "Voided",
]

VALID_DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def load_silver_month(year: int, month: int) -> pd.DataFrame:
    try:
        dt = DeltaTable(SILVER_URI, storage_options=STORAGE_OPTIONS)

        arrow_table = dt.to_pyarrow_table(
            filters=[
                ("source_year",  "=", year),
                ("source_month", "=", month),
            ]
        )

        return arrow_table.to_pandas()

    except Exception as e:
        raise RuntimeError(
            f"No Silver data found for {year}-{month:02d}. "
            f"Run python transformations/silver/taxi.py --year {year} "
            f"--month-start {month} --month-end {month} first.\nError: {e}"
        )


def save_validation_report(result: CheckpointResult, year: int, month: int):
    report_name    = f"{year}_{month:02d}.json"
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
    print(f"  {'=' * 55}")


def print_validation_report(result: CheckpointResult, year: int, month: int):
    print(f"\n  {'=' * 55}")
    print(f"  SILVER VALIDATION — {calendar.month_name[month]} {year}")
    print(f"  {'=' * 55}")

    validation_results = list(result.run_results.values())

    if validation_results:
        expectations_results = validation_results[0]["results"]
        passed = sum(1 for r in expectations_results if r["success"])
        total  = len(expectations_results)

        print(f"  Checks passed : {passed} / {total}")
        print(f"  Status        : {'✅ ALL CHECKS PASSED' if result.success else '❌ VALIDATION FAILED'}")

        if not result.success:
            print(f"\n  Failed checks:")
            for r in expectations_results:
                if not r["success"]:
                    exp_type = r["expectation_config"]["type"]
                    column   = r["expectation_config"]["kwargs"].get("column", "table-level")
                    observed = r.get("result", {}).get("observed_value", "N/A")
                    print(f"    ✗ {exp_type}")
                    print(f"      Column   : {column}")
                    print(f"      Observed : {observed}")


# ---------------------------------------------------------------------------
# GREAT EXPECTATIONS TEST SUITE
# ---------------------------------------------------------------------------

def build_silver_suite(context: AbstractDataContext, suite_name: str) -> gx.ExpectationSuite:
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    # ------------------------------------------------------------------
    # Rule 1: Row count
    # Silver rows will always be <= bronze due to cleaning rules
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=MIN_ROWS_PER_MONTH,
            max_value=MAX_ROWS_PER_MONTH,
        )
    )

    # ------------------------------------------------------------------
    # Rule 2: Schema — every expected column must exist
    # ------------------------------------------------------------------
    for column in EXPECTED_COLUMNS:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=column)
        )

    # ------------------------------------------------------------------
    # Rule 3: Null checks — critical columns (no null)
    # ------------------------------------------------------------------
    for column in COLUMNS_CRITICAL_NOT_NULL:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column,
                mostly=EXPECTED_NON_NULL_PERCENTAGE,
            )
        )

    # ------------------------------------------------------------------
    # Rule 4: Null checks — soft columns (75% non-null)
    # Optional fields like airport_fee, trip_speed_mph are legitimately sparse
    # ------------------------------------------------------------------
    for column in COLUMNS_SOFT_NOT_NULL:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column,
                mostly=EXPECTED_NON_NULL_PERCENTAGE_SOFT,
            )
        )

    # ------------------------------------------------------------------
    # Rule 5: Value range checks — must match silver_transform.py thresholds
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="fare_amount",
            min_value=MIN_FARE,
            max_value=MAX_FARE,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="trip_distance",
            min_value=MIN_TRIP_DISTANCE,
            max_value=None,             # no upper bound enforced in transform
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=MIN_PASSENGERS,
            max_value=MAX_PASSENGERS,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="trip_duration_minutes",
            min_value=1,                # enforced by Rule 6 in transform
            max_value=1440,             # 24 hours — anything longer is suspect
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pickup_hour",
            min_value=0,
            max_value=23,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pickup_day_of_week",
            min_value=0,
            max_value=7,
        )
    )

    # ------------------------------------------------------------------
    # Rule 6: Pickup must always be before dropoff
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="tpep_dropoff_datetime",
            column_B="tpep_pickup_datetime",
        )
    )

    # ------------------------------------------------------------------
    # Rule 7: Categorical set checks — derived columns
    # Any broken CASE logic in the transform shows up here immediately
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="payment_method",
            value_set=VALID_PAYMENT_METHODS,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="day_name",
            value_set=VALID_DAY_NAMES,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="is_weekend",
            value_set=[True, False],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="source_month",
            value_set=list(range(1, 13)),
        )
    )

    return suite


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run Silver validation suite for NYC Taxi data")
    parser.add_argument("--year",        type=int, default=2023, help="Year to validate (default: 2023)")
    parser.add_argument("--month-start", type=int, default=None, help="First month in range. Omit for full year")
    parser.add_argument("--month-end",   type=int, default=None, help="Last month in range. Omit for full year")

    args = parser.parse_args()

    #Make the month range from arguments
    if args.month_start is not None and args.month_end is not None:
        if args.month_start > args.month_end:
            print(f"ERROR: Month start ({args.month_start}) is greater than month end ({args.month_end})")
            exit(1)
        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)

    year = args.year

    print(f"\nNYC Taxi Silver Validation Suite")
    print(f"Year: {year} | Months: {', '.join(calendar.month_name[m] for m in months)}")

    #This stores our validation checks telling us what passed and failed
    results = {"passed": [], "failed": []}

    #Create Great Expectations context
    #Ensures it is not persisted to the disk
    context     = gx.get_context()

    #Register the data source
    data_source = context.data_sources.add_pandas(name="silver_taxi")
    data_asset  = data_source.add_dataframe_asset(name="silver_monthly_batch")

    for month in months:
        print(f"\nBeginning validation of {calendar.month_name[month]} {year}")
        print(f"Fetching delta table from MinIO Silver Bucket on 'source_year' and 'source_month' partition...")

        try:
            pandas_df = load_silver_month(year, month)
            print(f"✅ Successfully loaded Silver delta table — {pandas_df.shape[0]:,} rows")
        except RuntimeError as e:
            print(f"  [ERROR] {e}")
            results["failed"].append(f"{year}_{month:02d}")
            continue

        #Cleanup old batch definitions before creating this one
        #This is important for any re runs that ever happen
        try:
            data_asset.batch_definitions.delete(f"silver_taxi_{year}_{month:02d}")
        except Exception:
            pass
        
        #Attach our defined suite to the context
        suite = build_silver_suite(context, f"silver_taxi_{year}_{month:02d}")

        # Add cleanup before each registration by deleting old
        # validation definitions and checkpoints
        #This is important for any re runs that ever happen
        try:
            context.validation_definitions.delete(f"silver_validation_{year}_{month:02d}")
        except Exception:
            pass

        try:
            context.checkpoints.delete(f"silver_checkpoint_{year}_{month:02d}")
        except Exception:
            pass

        #Register the batch for the suite
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            f"silver_taxi_{year}_{month:02d}"
        )

        #Link the batch to the suite
        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name  = f"silver_validation_{year}_{month:02d}",
                data  = batch_definition,
                suite = suite,
            )
        )

        #Create checkpoints between each month batch
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name                   = f"silver_checkpoint_{year}_{month:02d}",
                validation_definitions = [validation_definition],
                result_format          = {"result_format": "SUMMARY"},
            )
        )

        #Execute the test suite
        result = checkpoint.run(
            batch_parameters={"dataframe": pandas_df}
        )

        print_validation_report(result, year, month)
        save_validation_report(result, year, month)

        if result.success:
            results["passed"].append(f"{year}_{month:02d}")
        else:
            results["failed"].append(f"{year}_{month:02d}")

    #Print overall summary of the validation
    print(f"\n{'=' * 55}")
    print(f"SILVER VALIDATION COMPLETE")
    print(f"{'=' * 55}")
    print(f"  Passed : {len(results['passed'])} month/s")
    print(f"  Failed : {len(results['failed'])} month/s")

    if results["failed"]:
        print(f"  Failed : {', '.join(results['failed'])}")
        print(f"\n  ❌ Pipeline blocked. Fix Silver issues before running Gold.")
        sys.exit(1)
    else:
        print(f"\n  ✅ All checks passed. Safe to run Gold layer transforms.")

    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()