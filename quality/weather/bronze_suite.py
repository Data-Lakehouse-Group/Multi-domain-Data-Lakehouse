"""
NOAA Weather Bronze Validation Suite (POST 2000 SCHEMA)
==================================
Validates that Bronze ingestion completed successfully through the following:
    -The schema matches our expected schema with audited columns included
    -The rows are between 100,000 and 6,000,000 per year
    -Ensure sepecified columns have non null values for a given percentage of the column


Runs AFTER bronze_ingest.py, BEFORE silver_transform.py

Usage:
    python quality/weather/bronze_suite.py
    python quality/weather/bronze_suite.py --year-start 2020 --year-end 2023
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
BRONZE_URI = "s3://bronze/weather"

#URI in MinIO to store reports
REPORT_URI = "s3://artifacts/great_expectations/reports/weather/bronze"

# MinIO connection
STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : "us-east-1",
    "aws_s3_allow_unsafe_rename": "true",
}

# Expected columns from NOAA Weather schema (post 2000)
EXPECTED_COLUMNS = [
    "STATION",
    "DATE",
    "DEWP",
    "DEWP_ATTRIBUTES",
    "WDSP",
    "WDSP_ATTRIBUTES",
    "TEMP", 
    "TEMP_ATTRIBUTES",
    "MXSPD",
    "MAX",
    "MAX_ATTRIBUTES",
    "GUST",
    "MIN",
    "MIN_ATTRIBUTES",
    "VISIB",
    "VISIB_ATTRIBUTES",
    "PRCP",
    "PRCP_ATTRIBUTES",
    "SNDP", 
    "FRSHTT",
    "STP", 
    "STP_ATTRIBUTES",
    "SLP",
    "SLP_ATTRIBUTES",

    # Audit columns added by bronze_ingest.py
    "ingested_at",
    "source_file",
    "source_year",
    "source_month"
]

# Expected row count range per year
MIN_ROWS_PER_YEAR = 1_000_000     
MAX_ROWS_PER_YEAR = 5_000_000   

EXPECTED_NON_NULL_PERCENTAGE = 0.9

COLUMNS_TO_CHECK_NULLS = [
    "STATION",
    "DATE",
    "DEWP",
    "WDSP",
    "TEMP", 
    "MXSPD",
    "MAX",
    "MIN",
    "VISIB",
    "PRCP", 
    "source_year", 
    "source_month"
]

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def load_bronze_year(year: int) -> pd.DataFrame:
    try:
        dt = DeltaTable(BRONZE_URI, storage_options=STORAGE_OPTIONS)
        
        #Filters the delta table by our partitioned columns
        #We added source_year and source_month in the bronze ingestion
        arrow_table = dt.to_pyarrow_table(
            filters=[
                ("source_year", "=", year),
            ]
        )

        return arrow_table.to_pandas()
    
    except Exception as e:
        raise RuntimeError(
            f"No Bronze data found for {year}. "
            f"Could not partition table on audit 'source_year'columns"
            f"Run python ingestion/weather/bronze_ingestion.py --year-start {year} --year-end {year} first.\nError: {e}"
        )

def save_validation_report(result: CheckpointResult, year: int):
    report_name    = f"{year}.json"
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
        Key         = f"great_expectations/reports/weather/bronze/{report_name}",
        Body        = report_content.encode("utf-8"),
        ContentType = "application/json",
    )

    print(f"\n  Report saved : {REPORT_URI}/{report_name}")
    print(f"  {'=' * 55}")

def print_validation_report(result: CheckpointResult, year: int):
    print(f"\n  {'=' * 55}")
    print(f"  BRONZE VALIDATION —  {year}")
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
            min_value=MIN_ROWS_PER_YEAR,
            max_value=MAX_ROWS_PER_YEAR,
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
    parser.add_argument("--year-start", type=int, default=2023, help="First year in range")
    parser.add_argument("--year-end",   type=int, default=2023, help="Last year in range")

    args = parser.parse_args()

    # Build the year range
    if args.year_start > args.year_end:
            print(f"ERROR: Year start ({args.year_start}) is greater than year end ({args.year_end})")
            exit(1)
    years = range(args.year_start, args.year_end + 1)

    print(f"\nNOAA Weather Bronze Validation Suite")
    print(f"Years: {', '.join(str(y) for y in years)}")

    #This stores our validation checks telling us what passed and failed
    results = {"passed": [], "failed": []}

    #Create Great Expectations context
    #Ensures it is not persisted to the disk
    context = gx.get_context()

    #Register the data source
    data_source = context.data_sources.add_pandas(name="bronze_weather")
    data_asset = data_source.add_dataframe_asset(name="bronze_yearly_batch")

    for year in years:
        print(f"Beginning validation of year {year}")

        print(f"Fetching delta table from Minio Bronze Bucket on 'source_year' partition ")

        try:
            pandas_df = load_bronze_year(year)
            print(f"✅ Successfully Loaded delta table from Minio Bronze Bucket")
        except RuntimeError as e:
            print(f"  [ERROR] {e}")
            results["failed"].append(f"{year}")
            continue
        
        #Attach our defined suite to the context
        suite = build_bronze_suite(context, f"bronze_weather_{year}")

        #Cleanup old batch definitions before creating this one
        #This is important for any re runs that ever happen
        try:
            data_asset.batch_definitions.delete(f"bronze_{year}")
        except Exception:
            pass

        #Register the batch for the suite
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            f"bronze_{year}"
        )

        # Cleanup before each registration by deleting old
        # validation definitions and checkpoints
        #This is important for any re runs that ever happen
        try:
            context.validation_definitions.delete(f"bronze_validation_{year}")
        except Exception:
            pass

        try:
            context.checkpoints.delete(f"bronze_checkpoint_{year}")
        except Exception:
            pass

        #Link the batch to the suite
        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name = f"bronze_validation_{year}",
                data = batch_definition,
                suite = suite,
            )
        )

        #Create checkpoints between each year batch
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name = f"bronze_checkpoint_{year}",
                validation_definitions = [validation_definition],
                result_format = {"result_format": "SUMMARY"},
            )
        )

        #Execute the test suite
        result = checkpoint.run(
            batch_parameters={"dataframe": pandas_df}
        )

        #Prints the report to the screen
        print_validation_report(result, year)

        #Saves the validation report as a json file
        save_validation_report(result, year)

        if result.success:
            results["passed"].append(f"{year}")
        else:
            results["failed"].append(f"{year}")
            

    #Print overall summary of the validation
    print(f"\n{'=' * 55}")
    print(f"BRONZE VALIDATION COMPLETE")
    print(f"{'=' * 55}")
    print(f"  Passed : {len(results['passed'])} year/s")
    print(f"  Failed : {len(results['failed'])} year/s")

    if results["failed"]:
        print(f"  Failed : {', '.join(results['failed'])}")
        print(f"\n  ❌ Pipeline blocked. Fix Bronze issues before running Silver.")
        sys.exit(1)
    else:
        print(f"\n  ✅ All checks passed. Safe to run silver layer transforms")

    print(f"{'=' * 55}")

if __name__ == "__main__":
    main()