"""
NYC Taxi Bronze Validation Suite
==================================
Validates that Bronze ingestion completed successfully through the following:
    -The schema matches our expected schema with audited columns included
    -The rows are between 100,000 and 6,000,000 per month
    -Ensure sepecified columns have non null values for a given percentage of the column


Runs AFTER bronze_ingest.py, BEFORE silver_transform.py

Usage:
    python quality/taxi/bronze_suite.py
    python quality/taxi/bronze_suite.py --year 2023
    python quality/taxi/bronze_suite.py --year 2023 --month-start 1 --month-end 1
    python quality/taxi/bronze_suite.py --year 2023 --month-start 1 --month-end 6
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

# Bronze delta table URI in MinIO
BRONZE_URI = "s3://bronze/taxi"

#URI in MinIO to store reports
REPORT_URI = "s3://artifacts/great_expectations/reports/taxi/bronze"

# MinIO connection
STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : "us-east-1",
    "aws_s3_allow_unsafe_rename": "true",
}

# Expected columns from TLC Yellow Taxi schema
EXPECTED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",

    # Audit columns added by bronze_ingest.py
    "ingested_at",
    "source_file",
    "source_year",
    "source_month"
]

# Expected row count range per month
MIN_ROWS_PER_MONTH = 200_000     
MAX_ROWS_PER_MONTH = 6_000_000   

EXPECTED_NON_NULL_PERCENTAGE = 0.9
COLUMNS_TO_CHECK_NULLS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
    "trip_distance",
    "ingested_at", 
    "source_file", 
    "source_year", 
    "source_month"
]

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def load_bronze_month(year: int, month: int) -> pd.DataFrame:
    try:
        dt = DeltaTable(BRONZE_URI, storage_options=STORAGE_OPTIONS)
        
        #Filters the delta table by our partitioned columns
        #We added source_year and source_month in the bronze ingestion
        arrow_table = dt.to_pyarrow_table(
            filters=[
                ("source_year", "=", year),
                ("source_month", "=", month),
            ]
        )

        return arrow_table.to_pandas()
    
    except Exception as e:
        raise RuntimeError(
            f"No Bronze data found for {year}-{month:02d}. "
            f"Could not partition table on audit 'source_year' and 'source_month' columns"
            f"Run bronze_ingest.py --year {year} --month {month} first.\nError: {e}"
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
        Key         = f"great_expectations/reports/taxi/bronze/{report_name}",
        Body        = report_content.encode("utf-8"),
        ContentType = "application/json",
    )

    print(f"\n  Report saved : {REPORT_URI}/{report_name}")
    print(f"  {'=' * 55}")

def print_validation_report(result: CheckpointResult, year: int, month: int):
    print(f"\n  {'=' * 55}")
    print(f"  BRONZE VALIDATION — {calendar.month_name[month]} {year}")
    print(f"  {'=' * 55}")

    validation_results = list(result.run_results.values())

    if validation_results:
        expectations_results = validation_results[0]["results"]
        passed = sum(1 for r in expectations_results if r["success"])
        total  = len(expectations_results)

        print(f"  Checks passed : {passed} / {total}")
        print(f"  Status        : {'✅ ALL CHECKS PASSED' if result.success else '❌ VALIDATION FAILED'}")

        # Print details of any failed checks
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
# GREAT EXPECTATION TEST SUITE
# ---------------------------------------------------------------------------

def build_bronze_suite(context: AbstractDataContext, suite_name: str) -> gx.ExpectationSuite:
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=MIN_ROWS_PER_MONTH,
            max_value=MAX_ROWS_PER_MONTH,
        )
    )

    for column in EXPECTED_COLUMNS:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=column)
        )

    for column in COLUMNS_TO_CHECK_NULLS:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column,
                mostly=EXPECTED_NON_NULL_PERCENTAGE,
            )
        )

    return suite

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run Bronze validation suite for NYC Taxi data")
    parser.add_argument("--year",  type=int, default=2023, help="Year to validate (default: 2023)")
    parser.add_argument("--month-start", type=int, default=None, help="First month in range. Omit for full year")
    parser.add_argument("--month-end", type=int, default=None, help="Last month in range. Omit for full year")

    args = parser.parse_args()

    #Make the month range from arguments
    if args.month_start is not None and args.month_end is not None:
        if(args.month_start > args.month_end):
            print(f"ERROR: Month start range ({args.month_start}) is greater than month end range({args.month_end})")
            exit(1)

        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)
    
    year = args.year

    print(f"\nNYC Taxi Bronze Validation Suite")
    print(f"Year: {year} | Months: {', '.join(calendar.month_name[m] for m in months)}")

    #This stores our validation checks telling us what passed and failed
    results = {"passed": [], "failed": []}

    #Create Great Expectations context
    #Ensures it is not persisted to the disk
    context = gx.get_context()

    #Register the data source
    data_source = context.data_sources.add_pandas(name="bronze_taxi")
    data_asset = data_source.add_dataframe_asset(name="bronze_monthly_batch")


    for month in months:
        print(f"Beginning validation of month {calendar.month_name[month]} for year {year}")

        print(f"Fetching delta table from Minio Bronze Bucket on 'source_year' and 'source_month' partition ")

        try:
            pandas_df = load_bronze_month(year, month)
            print(f"✅ Successfully Loaded delta table from Minio Bronze Bucket")
        except RuntimeError as e:
            print(f"  [ERROR] {e}")
            results["failed"].append(f"{year}_{month:02d}")
            continue

        #Cleanup old batch definitions before creating this one
        #This is important for any re runs that ever happen
        try:
            data_asset.batch_definitions.delete(f"bronze_taxi_{year}_{month:02d}")
        except Exception:
            pass

        #Attach our defined suite to the context
        suite = build_bronze_suite(context, f"bronze_taxi_{year}_{month:02d}")

        # Add cleanup before each registration by deleting old
        # validation definitions and checkpoints
        #This is important for any re runs that ever happen
        try:
            context.validation_definitions.delete(f"bronze_validation_{year}_{month:02d}")
        except Exception:
            pass

        try:
            context.checkpoints.delete(f"bronze_checkpoint_{year}_{month:02d}")
        except Exception:
            pass

        #Register the batch for the suite
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            f"bronze_{year}_{month:02d}"
        )

        #Link the batch to the suite
        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name = f"bronze_validation_{year}_{month:02d}",
                data = batch_definition,
                suite = suite,
            )
        )

        #Create checkpoints between each month batch
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name = f"bronze_checkpoint_{year}_{month:02d}",
                validation_definitions = [validation_definition],
                result_format = {"result_format": "SUMMARY"},
            )
        )

        #Execute the test suite
        result = checkpoint.run(
            batch_parameters={"dataframe": pandas_df}
        )

        #Prints the report to the screen
        print_validation_report(result, year, month)

        #Saves the validation report as a json file
        save_validation_report(result, year, month)

        if result.success:
            results["passed"].append(f"{year}_{month:02d}")
        else:
            results["failed"].append(f"{year}_{month:02d}")
            

    #Print overall summary of the validation
    print(f"\n{'=' * 55}")
    print(f"BRONZE VALIDATION COMPLETE")
    print(f"{'=' * 55}")
    print(f"  Passed : {len(results['passed'])} month/s")
    print(f"  Failed : {len(results['failed'])} month/s")

    if results["failed"]:
        print(f"  Failed : {', '.join(results['failed'])}")
        print(f"\n  ❌ Pipeline blocked. Fix Bronze issues before running Silver.")
        sys.exit(1)
    else:
        print(f"\n  ✅ All checks passed. Safe to run silver layer transforms")

    print(f"{'=' * 55}")

if __name__ == "__main__":
    main()