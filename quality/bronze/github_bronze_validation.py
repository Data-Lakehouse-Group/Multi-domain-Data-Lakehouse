"""
GitHub Archive Bronze Validation
================================
Validates Bronze Delta table for schema, row counts, and null constraints.
"""

import calendar
import argparse
import json
import sys
from pathlib import Path

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import CheckpointResult
import pandas as pd
from deltalake import DeltaTable

BRONZE_URI = "s3://bronze/github_archive"
REPORT_DIR = Path("quality/reports/bronze/github")

STORAGE_OPTIONS = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name": "us-east-1",
    "allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

EXPECTED_COLUMNS = [
    "id", "type", "actor", "repo", "org", "payload", "public", "created_at",
    "ingested_at", "source_file", "source_year", "source_month", "source_day", "source_hour"
]
MIN_ROWS_PER_HOUR = 5000      # GH Archive typically ~10k-50k events per hour
MAX_ROWS_PER_HOUR = 150000
EXPECTED_NON_NULL_PERCENTAGE = 0.99
COLUMNS_TO_CHECK_NULLS = ["id", "type", "created_at", "ingested_at"]

def load_bronze_hour(year: int, month: int, day: int, hour: int) -> pd.DataFrame:
    dt = DeltaTable(BRONZE_URI, storage_options=STORAGE_OPTIONS)
    arrow_table = dt.to_pyarrow_table(
        filters=[
            ("source_year", "=", year),
            ("source_month", "=", month),
            ("source_day", "=", day),
            ("source_hour", "=", hour),
        ]
    )
    return arrow_table.to_pandas()

def save_validation_report(result: CheckpointResult, year: int, month: int, day: int, hour: int):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"bronze_{year}_{month:02d}_{day:02d}_{hour:02d}.json"
    with open(report_path, "w") as f:
        json.dump(result.describe_dict(), f, indent=2, default=str)
    print(f"  Report saved: {report_path}")

def print_validation_report(result: CheckpointResult, dt_str: str):
    print(f"\n  {'=' * 55}")
    print(f"  BRONZE VALIDATION — {dt_str}")
    print(f"  {'=' * 55}")
    vr = list(result.run_results.values())[0]["results"]
    passed = sum(1 for r in vr if r["success"])
    total = len(vr)
    print(f"  Checks passed: {passed}/{total}")
    print(f"  Status: {'✅ PASSED' if result.success else '❌ FAILED'}")
    if not result.success:
        print("\n  Failed checks:")
        for r in vr:
            if not r["success"]:
                exp = r["expectation_config"]["type"]
                col = r["expectation_config"]["kwargs"].get("column", "table")
                obs = r.get("result", {}).get("observed_value", "N/A")
                print(f"    ✗ {exp} | Column: {col} | Observed: {obs}")

def build_suite(context, suite_name: str) -> gx.ExpectationSuite:
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=MIN_ROWS_PER_HOUR, max_value=MAX_ROWS_PER_HOUR
        )
    )
    for col in EXPECTED_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))
    for col in COLUMNS_TO_CHECK_NULLS:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col, mostly=EXPECTED_NON_NULL_PERCENTAGE
            )
        )
    return suite

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date-hour", type=str, help="Single hour YYYY-MM-DD-HH")
    parser.add_argument("--date-hour-start", type=str, help="Start hour")
    parser.add_argument("--date-hour-end", type=str, help="End hour")
    args = parser.parse_args()

    # Build list of datetime tuples (year, month, day, hour)
    if args.date_hour_start and args.date_hour_end:
        from datetime import datetime, timedelta
        start = datetime.strptime(args.date_hour_start, "%Y-%m-%d-%H")
        end = datetime.strptime(args.date_hour_end, "%Y-%m-%d-%H")
        hours = []
        current = start
        while current <= end:
            hours.append((current.year, current.month, current.day, current.hour))
            current += timedelta(hours=1)
    elif args.date_hour:
        dt = datetime.strptime(args.date_hour, "%Y-%m-%d-%H")
        hours = [(dt.year, dt.month, dt.day, dt.hour)]
    else:
        print("ERROR: Provide --date-hour or range.")
        sys.exit(1)

    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="bronze_github")
    data_asset = data_source.add_dataframe_asset(name="bronze_hourly_batch")
    results = {"passed": [], "failed": []}

    for y, m, d, h in hours:
        dt_str = f"{y}-{m:02d}-{d:02d} hour {h}"
        print(f"\nValidating {dt_str}...")
        try:
            df = load_bronze_hour(y, m, d, h)
            print(f"✅ Loaded {len(df):,} rows")
        except Exception as e:
            print(f"❌ Failed to load: {e}")
            results["failed"].append(dt_str)
            continue

        suite = build_suite(context, f"github_bronze_{y}{m:02d}{d:02d}_{h:02d}")
        batch_def = data_asset.add_batch_definition_whole_dataframe(f"batch_{y}{m:02d}{d:02d}_{h:02d}")
        validation_def = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=f"val_{y}{m:02d}{d:02d}_{h:02d}",
                data=batch_def,
                suite=suite,
            )
        )
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name=f"cp_{y}{m:02d}{d:02d}_{h:02d}",
                validation_definitions=[validation_def],
                result_format={"result_format": "SUMMARY"},
            )
        )
        result = checkpoint.run(batch_parameters={"dataframe": df})
        print_validation_report(result, dt_str)
        save_validation_report(result, y, m, d, h)
        if result.success:
            results["passed"].append(dt_str)
        else:
            results["failed"].append(dt_str)

    print(f"\n{'='*55}\nSUMMARY: {len(results['passed'])} passed, {len(results['failed'])} failed")
    if results["failed"]:
        print(f"Failed hours: {', '.join(results['failed'])}")
        sys.exit(1)

if __name__ == "__main__":
    main()