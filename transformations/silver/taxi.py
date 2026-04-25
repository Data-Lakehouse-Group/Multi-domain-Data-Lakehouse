"""
NYC Taxi Silver Transformation
===============================
Reads from the Bronze Delta table, applies cleaning and validation
rules, derives new columns, and writes to the Silver Delta table.

Also optimizes data types.

Records that fail quality checks are dropped and counted.
No bad data passes through to Silver.

Input:  s3://bronze/taxi/yellow_tripdata/  (Delta table)
Output: s3://silver/taxi/yellow_tripdata/  (Delta table, cleaned)

Usage:
    python transformations/silver/taxi.py                                           # Performs Silver Transform on 2023 full year by default
    python transformations/silver/taxi.py --year 2023                               # Performs Silver Transform on specific year
    python transformations/silver/taxi.py --year 2023 --month-start 1 --month-end 1 # Performs Silver Transform on Jan 2023
    python transformations/silver/taxi.py --debug                                   # Run locally with debug paths
"""

import os
import argparse
import calendar

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BRONZE_URI = "s3://bronze/taxi/yellow_tripdata"
SILVER_URI = "s3://silver/taxi/yellow_tripdata"

# MinIO connection (secrets from environment, fallback for local dev)
STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : os.getenv("AWS_REGION", "us-east-1"),
    "aws_s3_allow_unsafe_rename": "true",
}

# Validation thresholds
MIN_FARE            = 0.0
MAX_FARE            = 500.0
MIN_TRIP_DISTANCE   = 0.0
MIN_PASSENGERS      = 1
MAX_PASSENGERS      = 6
MAX_TIP_PERCENTAGE  = 100.0       # cap tip at 100% of fare (remove obvious errors)

# DuckDB resource configuration
DUCKDB_THREADS      = 4
DUCKDB_MEMORY_LIMIT = "4GB"

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------
def table_exists(uri: str) -> bool:
    try:
        DeltaTable(uri, storage_options=STORAGE_OPTIONS)
        return True
    except Exception:
        return False

def read_bronze_table_for_month_year(year: int, month: int) -> pa.Table:
    dt = DeltaTable(BRONZE_URI, storage_options=STORAGE_OPTIONS)

    # Read only the partition for the given year/month
    arrow_table = dt.to_pyarrow_table(
        filters=[
            ("source_year",  "=", year),
            ("source_month", "=", month),
        ],
        # Optionally select only necessary columns to reduce memory
        # (all columns are used, so omit the parameter)
    )
    return arrow_table

def print_new_columns(previous_table: pa.Table, enriched_table: pa.Table):
    previous_cols = set(previous_table.column_names)
    enriched_cols = set(enriched_table.column_names)
    new_columns = enriched_cols - previous_cols
    if new_columns:
        for col in sorted(new_columns):
            col_type = enriched_table.schema.field(col).type
            print(f"        {col} ({col_type})")
    else:
        print("\n       No new columns added during enrichment")

# ---------------------------------------------------------------------------
# CLEANING FUNCTIONS
# ---------------------------------------------------------------------------

def apply_quality_checks_and_enrich(
    con: duckdb.DuckDBPyConnection, year: int, month: int
) -> pa.Table:
    """
    Apply all business rules, remove bad rows, and compute derived columns.
    Returns a clean, enriched Arrow table.
    """
    result = con.execute(f"""
        -- Step 1: Filter out invalid rows
        WITH cleaned AS (
            SELECT *
            FROM bronze_taxi
            WHERE
                -- Rule 1: No nulls on critical columns
                tpep_pickup_datetime IS NOT NULL
                AND tpep_dropoff_datetime IS NOT NULL
                AND PULocationID          IS NOT NULL
                AND DOLocationID          IS NOT NULL
                AND fare_amount           IS NOT NULL
                AND trip_distance         IS NOT NULL
                AND passenger_count       IS NOT NULL
                AND payment_type          IS NOT NULL

                -- Rule 2: Valid fare amount
                AND fare_amount > {MIN_FARE}
                AND fare_amount < {MAX_FARE}

                -- Rule 3: Valid trip distance
                AND trip_distance >= {MIN_TRIP_DISTANCE}

                -- Rule 4: Valid passenger count
                AND passenger_count >= {MIN_PASSENGERS}
                AND passenger_count <= {MAX_PASSENGERS}

                -- Rule 5: Pickup must be before dropoff
                AND tpep_pickup_datetime < tpep_dropoff_datetime

                -- Rule 6: Reasonable trip duration (at least 1 minute)
                AND DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) >= 1

                -- Rule 7: Record must belong to the correct year and month
                AND YEAR(tpep_pickup_datetime)  = {year}
                AND MONTH(tpep_pickup_datetime) = {month}

                -- Rule 8: Filter unrealistic tip amounts (tip > 100% of fare)
                AND (tip_amount IS NULL OR tip_amount <= fare_amount * {MAX_TIP_PERCENTAGE} / 100)
        ),

        -- Step 2: Pre-compute duration and day_of_week
        with_duration AS (
            SELECT
                *,
                DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
                CAST(DAYOFWEEK(tpep_pickup_datetime) AS UTINYINT) AS pickup_day_of_week
            FROM cleaned
        )

        -- Step 3: Add derived columns and cast to optimized types
        SELECT
            -- Original columns with optimised storage types
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            CAST(passenger_count  AS UTINYINT)  AS passenger_count,
            CAST(trip_distance    AS FLOAT)     AS trip_distance,
            CAST(PULocationID     AS USMALLINT) AS PULocationID,
            CAST(DOLocationID     AS USMALLINT) AS DOLocationID,
            CAST(payment_type     AS UTINYINT)  AS payment_type,
            CAST(fare_amount      AS FLOAT)     AS fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            store_and_fwd_flag,
            RatecodeID,
            source_year,
            source_month,
            source_file,
            ingested_at,
            trip_duration_minutes,
            pickup_day_of_week,

            -- Column 1: Trip speed in mph (null if duration zero)
            CASE
                WHEN trip_duration_minutes > 0
                THEN CAST(trip_distance / (trip_duration_minutes / 60.0) AS FLOAT)
                ELSE NULL
            END AS trip_speed_mph,

            -- Column 2: Hour of pickup
            CAST(HOUR(tpep_pickup_datetime) AS UTINYINT) AS pickup_hour,

            -- Column 3: Audit timestamp for this silver run
            CURRENT_TIMESTAMP AS processed_at,

            -- Column 4: Payment method description
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided'
            END AS payment_method,

            -- Column 5: Day name
            DAYNAME(tpep_pickup_datetime) AS day_name,

            -- Column 6: Is weekend flag
            CASE
                WHEN pickup_day_of_week IN (1, 0) THEN true
                ELSE false
            END AS is_weekend,

            -- Column 7: Pickup date
            DATE(tpep_pickup_datetime) AS pickup_date,

            -- Column 8: Is shared ride (more than 1 passenger)
            passenger_count > 1 AS is_shared_ride

        FROM with_duration
    """).fetch_arrow_table()

    return result


# ---------------------------------------------------------------------------
# CORE TRANSFORMATION FUNCTION
# ---------------------------------------------------------------------------

def transform_month(year: int, month: int) -> bool:
    month_name = calendar.month_name[month]
    print(f"\n[SILVER] Transforming {month_name} {year}")

    con = duckdb.connect()
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")

    try:
        # Step 1: Read Bronze partition
        print(f"    Reading Bronze data for {year}-{month:02d}...")
        bronze_pa_table = read_bronze_table_for_month_year(year, month)
        print(f"    Bronze rows read: {bronze_pa_table.num_rows:,}")

        # Step 2: Clean and enrich
        print(f"    Applying quality checks and feature engineering...")
        con.register("bronze_taxi", bronze_pa_table)
        silver_table = apply_quality_checks_and_enrich(con, year, month)

        print(f"    Rows dropped : {(bronze_pa_table.num_rows - silver_table.num_rows):,}")
        print(f"    Columns added:")
        print_new_columns(bronze_pa_table, silver_table)

        # Step 3: Write to Silver Delta table
        print(f"\n   Writing to Silver MinIO Bucket...")

        if table_exists(SILVER_URI):
            write_deltalake(
                SILVER_URI,
                silver_table,
                mode='overwrite',
                predicate=f"source_year = {year} AND source_month = {month}",
                schema_mode="merge",
                storage_options=STORAGE_OPTIONS,
            )
        else:
            write_deltalake(
                SILVER_URI,
                silver_table,
                mode='overwrite',
                partition_by=["source_year", "source_month"],
                schema_mode="overwrite",
                storage_options=STORAGE_OPTIONS,
            )

        print(f"   Silver write complete for {month_name} {year}\n")
        return True

    except Exception as e:
        print(f"[ERROR] {e}")
        return False

    finally:
        con.close()


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Transform NYC Taxi Bronze → Silver")
    parser.add_argument("--year",        type=int, default=2023)
    parser.add_argument("--month-start", type=int, default=None, help="First month in range")
    parser.add_argument("--month-end",   type=int, default=None, help="Last month in range")
    parser.add_argument("--debug",       action="store_true", help="Run locally (no effect on paths here)")
    args = parser.parse_args()

    # Month range
    if args.month_start and args.month_end:
        if args.month_start > args.month_end:
            print(f"ERROR: Month start range ({args.month_start}) is greater than month end range({args.month_end})")
            exit(1)
        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)

    print(f"\nNYC Taxi Silver Transformation")
    print(f"Year: {args.year} | Months: {', '.join(str(m) for m in months)}")

    results = {"success": [], "failed": []}

    for month in months:
        month_label = f"{args.year}-{month:02d}"
        if transform_month(args.year, month):
            results["success"].append(month_label)
        else:
            results["failed"].append(month_label)

    # Final summary
    print(f"\n{'=' * 55}")
    print(f"SILVER TRANSFORMATION SUMMARY")
    print(f"{'=' * 55}")
    print(f"  Successful : {len(results['success'])} months")
    print(f"  Failed     : {len(results['failed'])} months")
    if results["failed"]:
        print(f"  Failed     : {', '.join(results['failed'])}")
    print(f"  Silver table: {SILVER_URI}")
    print(f"{'=' * 55}")

    if results["failed"]:
        exit(1)


if __name__ == "__main__":
    main()