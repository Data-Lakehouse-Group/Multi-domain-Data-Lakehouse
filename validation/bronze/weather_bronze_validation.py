"""
NOAA Weather Bronze Validation
==============================
Validates Bronze Delta table per year.
"""

import argparse
import json
import sys
from pathlib import Path
import great_expectations as gx
import pandas as pd
from deltalake import DeltaTable

BRONZE_URI = "s3://bronze/weather"
REPORT_DIR = Path("quality/reports/bronze/weather")
STORAGE_OPTIONS = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name": "us-east-1",
    "allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

EXPECTED_COLUMNS = [
    "STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME",
    "TEMP", "TEMP_ATTRIBUTES", "DEWP", "DEWP_ATTRIBUTES", "SLP", "SLP_ATTRIBUTES",
    "STP", "STP_ATTRIBUTES", "VISIB", "VISIB_ATTRIBUTES", "WDSP", "WDSP_ATTRIBUTES",
    "MXSPD", "GUST", "MAX", "MAX_ATTRIBUTES", "MIN", "MIN_ATTRIBUTES", "PRCP",
    "PRCP_ATTRIBUTES", "SNDP", "FRSHTT", "ingested_at", "source_file", "source_year", "source_month"
]
MIN_ROWS_PER_YEAR = 1_000_000
MAX_ROWS_PER_YEAR = 5_000_000
NON_NULL_COLS = ["STATION", "DATE", "TEMP", "ingested_at"]

def load_bronze_year(year: int) -> pd.DataFrame:
    dt = DeltaTable(BRONZE_URI, storage_options=STORAGE_OPTIONS)
    arrow = dt.to_pyarrow_table(filters=[("source_year", "=", year)])
    return arrow.to_pandas()

def validate_year(context, year: int, df: pd.DataFrame):
    suite_name = f"bronze_weather_{year}"
    try:
        context.suites.delete(suite_name)
    except:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=MIN_ROWS_PER_YEAR, max_value=MAX_ROWS_PER_YEAR
        )
    )
    for col in EXPECTED_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))
    for col in NON_NULL_COLS:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col, mostly=0.99)
        )

    data_source = context.data_sources.add_pandas(name=f"weather_{year}")
    data_asset = data_source.add_dataframe_asset(name="batch")
    batch_def = data_asset.add_batch_definition_whole_dataframe("whole")
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"val_{year}", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name=f"cp_{year}", validation_definitions=[val_def])
    )
    return checkpoint.run(batch_parameters={"dataframe": df})

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Single year")
    parser.add_argument("--year-start", type=int, help="Start year")
    parser.add_argument("--year-end", type=int, help="End year")
    args = parser.parse_args()
    if args.year_start and args.year_end:
        years = list(range(args.year_start, args.year_end + 1))
    elif args.year:
        years = [args.year]
    else:
        print("Provide --year or --year-start/--year-end")
        sys.exit(1)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    context = gx.get_context()
    all_passed = True
    for year in years:
        print(f"\nValidating {year}...")
        try:
            df = load_bronze_year(year)
            print(f"  Loaded {len(df):,} rows")
        except Exception as e:
            print(f"  ❌ Failed: {e}")
            all_passed = False
            continue
        result = validate_year(context, year, df)
        passed = result.success
        print(f"  Status: {'✅ PASSED' if passed else '❌ FAILED'}")
        report_path = REPORT_DIR / f"{year}.json"
        with open(report_path, "w") as f:
            json.dump(result.describe_dict(), f, indent=2, default=str)
        if not passed:
            all_passed = False

    if not all_passed:
        sys.exit(1)
    print("\n✅ All weather bronze validations passed.")

if __name__ == "__main__":
    main()