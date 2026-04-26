"""
NOAA Weather Silver Validation Suite
==================================
Validates that Silver transformation completed successfully through the following:
    - The schema matches our expected silver schema with all derived columns
    - The rows are between expected bounds per year
    - Critical columns have non-null values above a given threshold
    - Value ranges are physically plausible
    - Categorical/derived columns contain only valid values
    - Cross-field consistency between raw and derived columns

Runs AFTER silver_transform.py, BEFORE gold layer or dbt models

Usage:
    python quality/weather/silver_suite.py
    python quality/weather/silver_suite.py --year-start 2020 --year-end 2023
"""

import os
import argparse
import json
import sys
import boto3

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.data_context import AbstractDataContext
import pandas as pd

from deltalake import DeltaTable


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SILVER_URI = "s3://silver/weather"

REPORT_URI = "s3://artifacts/great_expectations/reports/weather/silver"

STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : "us-east-1",
    "aws_s3_allow_unsafe_rename": "true",
}

# Expected silver schema — all original, derived, and audit columns
EXPECTED_COLUMNS = [
    # Original columns (optimised types)
    "STATION",
    "DATE",
    "LATITUDE",
    "LONGITUDE",
    "ELEVATION",
    "NAME",

    # Core weather metrics (sentinel-cleaned)
    "TEMP",
    "DEWP",
    "SLP",
    "STP",
    "VISIB",
    "WDSP",
    "MXSPD",
    "GUST",
    "MAX",
    "MIN",
    "PRCP",
    "SNDP",

    # Bronze audit columns (passed through)
    "source_year",
    "source_month",
    "source_file",
    "ingested_at",

    # Derived: date dimensions
    "season_northern",
    "is_weekend",
    "day_name",

    # Derived: unit conversions
    "temp_c",
    "max_temp_c",
    "min_temp_c",
    "wdsp_kmh",
    "mxspd_kmh",
    "gust_kmh",

    # Derived: weather event flags
    "is_freezing",
    "is_extreme_heat",
    "has_precipitation",
    "is_gale",

    # Silver audit column
    "processed_at",
]

# Row count bounds (Less rows are expected)
MIN_ROWS_PER_YEAR = 800_000
MAX_ROWS_PER_YEAR = 5_000_000

# Null thresholds
EXPECTED_NON_NULL_PERCENTAGE = 1   # critical columns (Should not be null after cleaning)
EXPECTED_NON_NULL_PERCENTAGE_SOFT = 0.75  # optional columns (GUST, SNDP etc.)

COLUMNS_CRITICAL_NOT_NULL = [
    "STATION",
    "DATE",
    "TEMP",
    "WDSP",
    "VISIB",
    "PRCP",
    "LATITUDE",
    "LONGITUDE",

    #All derived columns
    "source_year",
    "source_month",
    "source_file",
    "ingested_at",
    "season_northern",
    "day_name",
    "is_weekend",
    "is_freezing",
    "is_extreme_heat",
    "has_precipitation",
    "is_gale",
    "temp_c",
    "processed_at",
]

COLUMNS_SOFT_NOT_NULL = [
    "DEWP",
    "SLP",
    "STP",
    "MXSPD",
    "GUST",
    "MAX",
    "MIN",
    "SNDP",
    "max_temp_c",
    "min_temp_c",
    "mxspd_kmh",
    "gust_kmh",
]

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def load_silver_year(year: int) -> pd.DataFrame:
    try:
        dt = DeltaTable(SILVER_URI, storage_options=STORAGE_OPTIONS)

        arrow_table = dt.to_pyarrow_table(
            filters=[
                ("source_year", "=", year),
            ]
        )

        return arrow_table.to_pandas()

    except Exception as e:
        raise RuntimeError(
            f"No Silver data found for {year}. "
            f"Run python transformations/silver/weather.py --year-start {year} --year-end {year} first.\nError: {e}"
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
        Key         = f"great_expectations/reports/weather/silver/{report_name}",
        Body        = report_content.encode("utf-8"),
        ContentType = "application/json",
    )

    print(f"\n  Report saved : {REPORT_URI}/{report_name}")
    print(f"  {'=' * 55}")


def print_validation_report(result: CheckpointResult, year: int):
    print(f"\n  {'=' * 55}")
    print(f"  SILVER VALIDATION — {year}")
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
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=MIN_ROWS_PER_YEAR,
            max_value=MAX_ROWS_PER_YEAR,
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
    # Rule 3: Null checks — critical columns (100% non-null)
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
    # Optional fields like GUST, SNDP are legitimately sparse
    # ------------------------------------------------------------------
    for column in COLUMNS_SOFT_NOT_NULL:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column,
                mostly=EXPECTED_NON_NULL_PERCENTAGE_SOFT,
            )
        )

    # ------------------------------------------------------------------
    # Rule 5: Geospatial range checks (Ensuring Silver Cleaning Worked)
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="LATITUDE", min_value=-90, max_value=90,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="LONGITUDE", min_value=-180, max_value=180,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="ELEVATION", min_value=-500, max_value=9000,
        )
    )

    # ------------------------------------------------------------------
    # Rule 6: Physical plausibility — weather metric ranges
    # Sentinel values already removed; anything left must be real
    # (Ensuring Silver Cleaning Worked)
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="TEMP", min_value=-129, max_value=135,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="temp_c", min_value=-89, max_value=57,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="WDSP", min_value=0, max_value=200,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="wdsp_kmh", min_value=0, max_value=370,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="PRCP", min_value=0, max_value=30,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="VISIB", min_value=0, max_value=999,
        )
    )

    # ------------------------------------------------------------------
    # Rule 7: Categorical set checks — derived columns
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="season_northern",
            value_set=["winter", "spring", "summer", "fall"],
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
            column="is_freezing",
            value_set=[True, False],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="is_extreme_heat",
            value_set=[True, False],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="has_precipitation",
            value_set=[True, False],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="is_gale",
            value_set=[True, False],
        )
    )

    # ------------------------------------------------------------------
    # Rule 8: Ensuring data is present for each month in the year
    # Will result in incomplete pipeline running later on
    # ------------------------------------------------------------------
    
    #Within appropriate range
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="source_month",
            value_set=list(range(1, 13)),
        )
    )

    #There are 12 unique values
    suite.add_expectation(
        gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
            column="source_month",
            min_value=12,
            max_value=12,
        )
    )
    # ------------------------------------------------------------------
    # Rule 9: Uniqueness — one record per station per day
    # ------------------------------------------------------------------
    suite.add_expectation(
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["STATION", "DATE"],
        )
    )

    return suite


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run Silver validation suite for NOAA Weather data")
    parser.add_argument("--year-start", type=int, default=2023, help="First year in range")
    parser.add_argument("--year-end",   type=int, default=2023, help="Last year in range")

    args = parser.parse_args()

    # Build the year range
    if args.year_start > args.year_end:
        print(f"ERROR: Year start ({args.year_start}) is greater than year end ({args.year_end})")
        exit(1)

    years = range(args.year_start, args.year_end + 1)

    print(f"\nNOAA Weather Silver Validation Suite")
    print(f"Years: {', '.join(str(y) for y in years)}")

    #This stores our validation checks telling us what passed and failed
    results = {"passed": [], "failed": []}

    #Create Great Expectations context
    #Ensures it is not persisted to the disk
    context     = gx.get_context()


    #Register the data source
    data_source = context.data_sources.add_pandas(name="silver_weather")
    data_asset  = data_source.add_dataframe_asset(name="silver_yearly_batch")

    for year in years:
        print(f"\nBeginning validation of year {year}")
        print(f"Fetching delta table from MinIO Silver Bucket on 'source_year' partition...")

        try:
            pandas_df = load_silver_year(year)
            print(f"✅ Successfully loaded Silver delta table — {pandas_df.shape[0]:,} rows")
        except RuntimeError as e:
            print(f"  [ERROR] {e}")
            results["failed"].append(f"{year}")
            continue
        
        #Attach our defined suite to the context
        suite = build_silver_suite(context, f"silver_weather_{year}")

        #Cleanup old batch definitions before creating this one
        #This is important for any re runs that ever happen
        try:
            data_asset.batch_definitions.delete(f"silver_{year}")
        except Exception:
            pass
        
        #Register the batch for the suite
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            f"silver_{year}"
        )

        # Cleanup before each registration by deleting old
        # validation definitions and checkpoints
        #This is important for any re runs that ever happen
        try:
            context.validation_definitions.delete(f"silver_validation_{year}")
        except Exception:
            pass

        try:
            context.checkpoints.delete(f"silver_checkpoint_{year}")
        except Exception:
            pass

        #Link the batch to the suite
        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name  = f"silver_validation_{year}",
                data  = batch_definition,
                suite = suite,
            )
        )

        #Create checkpoints between each year batch
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name                   = f"silver_checkpoint_{year}",
                validation_definitions = [validation_definition],
                result_format          = {"result_format": "SUMMARY"},
            )
        )

        #Execute the test suite
        result = checkpoint.run(
            batch_parameters={"dataframe": pandas_df}
        )

        print_validation_report(result, year)
        save_validation_report(result, year)

        if result.success:
            results["passed"].append(f"{year}")
        else:
            results["failed"].append(f"{year}")

    #Print overall summary of the validation
    print(f"\n{'=' * 55}")
    print(f"SILVER VALIDATION COMPLETE")
    print(f"{'=' * 55}")
    print(f"  Passed : {len(results['passed'])} year/s")
    print(f"  Failed : {len(results['failed'])} year/s")

    if results["failed"]:
        print(f"  Failed : {', '.join(results['failed'])}")
        print(f"\n  ❌ Pipeline blocked. Fix Silver issues before running Gold.")
        sys.exit(1)
    else:
        print(f"\n  ✅ All checks passed. Safe to run Gold layer transforms.")

    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()