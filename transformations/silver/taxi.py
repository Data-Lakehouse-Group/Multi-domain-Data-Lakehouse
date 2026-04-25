"""
NYC Taxi Silver Transformation
===============================
Reads from the Bronze Delta table, applies cleaning and validation
rules, derives new columns, and writes to the Silver Delta table.

Also optimizes data types

Records that fail quality checks are dropped and counted.
No bad data passes through to Silver.

Input:  s3://bronze/taxi/yellow_tripdata/  (Delta table)
Output: s3://silver/taxi/yellow_tripdata/  (Delta table, cleaned)

Usage:
    python transformations/silver/taxi.py                                           #Performs Silver Transform on 2023 full year by default
    python transformations/silver/taxi.py --year 2023                               #Performs Silver Transform on specific year
    python transformations/silver/taxi.py --year 2023 --month-start 1 --month-end 1 #Performs Silver Transform on Jan 2023 
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

BRONZE_URI = "s3://bronze/taxi"
SILVER_URI = "s3://silver/taxi"

# MinIO connection
STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : "us-east-1",
    "aws_s3_allow_unsafe_rename": "true",
}

# Validation thresholds
MIN_FARE            = 0.0
MAX_FARE            = 500.0
MIN_TRIP_DISTANCE   = 0.0
MIN_PASSENGERS      = 1
MAX_PASSENGERS      = 6

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
    dt = DeltaTable(BRONZE_URI, storage_options = STORAGE_OPTIONS)

    #Get the partitioned files on year and month
    arrow_table = dt.to_pyarrow_table(
        filters=[
            ("source_year",  "=", year),
            ("source_month", "=", month),
        ],
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

    result = con.execute(f"""

        -- Step 1: Apply quality rules to bronze data, dropping any rows that fail
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
        ),

        -- Step 2: Pre-compute trip duration so it can be reused in speed calculation
        --         Along with day_of_week
        with_duration AS (
            SELECT
                *,
                DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
                CAST(DAYOFWEEK(tpep_pickup_datetime)AS UTINYINT) AS pickup_day_of_week
            FROM cleaned
        )

        -- Step 3: Add all remaining derived and audit columns
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
            CAST(pickup_day_of_week AS UTINYINT) AS pickup_day_of_week,

            -- Column 1: Trip speed in mph, null if duration is zero to avoid division error
            CASE
                WHEN trip_duration_minutes > 0
                THEN CAST(trip_distance / (trip_duration_minutes / 60.0) AS FLOAT)
                ELSE NULL
            END AS trip_speed_mph,

            -- Column 2: Hour of pickup for time-of-day analysis
            CAST(HOUR(tpep_pickup_datetime) AS UTINYINT) AS pickup_hour,

            -- Column 3: Audit timestamp for when this silver processing ran
            CURRENT_TIMESTAMP AS processed_at,

            -- Column 4: Payment Method
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided'
            END AS payment_method,

            -- Column 5: Day of Week
            DAYNAME(tpep_pickup_datetime) AS day_name,

            -- Column 6: Is Weekend
            CASE
                WHEN pickup_day_of_week IN (1, 0) THEN true
                ELSE false
            END AS is_weekend,

            -- Column 7: Pickup Date
            DATE(tpep_pickup_datetime) AS pickup_date
            
        FROM with_duration

    """).fetch_arrow_table()

    return result

# ---------------------------------------------------------------------------
# CORE TRANSFORMATION FUNCTION
# ---------------------------------------------------------------------------

def transform_month(year: int, month: int) -> bool:
    month_name = calendar.month_name[month]
    print(f"\n[SILVER] Transforming {month_name} {year}")

    con = duckdb.connect()  # One connection for this month's entire lifecycle

    #Optimize memory and threads used
    con.execute("SET threads TO 4") 
    con.execute("SET memory_limit = '4GB'")
    
    try:
        #Step 1: Reading the appropriate month and year
        #partition from the Bronze Delta Table
        print(f"    Reading Bronze data for {year}-{month:02d}...")
        bronze_pa_table = read_bronze_table_for_month_year(year, month)
        print(f"    Bronze rows read: {bronze_pa_table.num_rows:,}")

        #Step 2: Apply quality checks and feature engineering in a single pass
        print(f"    Applying quality checks and feature engineering...")
        con.register("bronze_taxi", bronze_pa_table)
        silver_table = apply_quality_checks_and_enrich(con, year, month)

        print(f"    Rows dropped : {(bronze_pa_table.num_rows - silver_table.num_rows):,}")
        print(f"    Columns added:")
        print_new_columns(bronze_pa_table, silver_table)

        #Step 3: Write to Silver Delta Table
        print(f"\n   Writing to Silver Minio Bucket...")

        if table_exists(SILVER_URI):
            write_deltalake(
                SILVER_URI,
                silver_table,
                mode = 'overwrite',
                predicate=f"source_year = {year} AND source_month = {month}",
                schema_mode="merge", 
                storage_options= STORAGE_OPTIONS,
            )
        else:
            write_deltalake(
                SILVER_URI,
                silver_table,
                mode = 'overwrite',
                partition_by=["source_year", "source_month"],
                schema_mode="overwrite",
                storage_options= STORAGE_OPTIONS,
            )

        print(f"   Silver write complete for {month_name} {year} \n")
        return True
    except Exception as e:
        print(f"[ERROR] {e}")
        return False
    
    finally:
        con.close() # Close even on error


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Transform NYC Taxi Bronze → Silver")
    parser.add_argument("--year",        type=int, default=2023)
    parser.add_argument("--month-start", type=int, default=None, help="First month in range")
    parser.add_argument("--month-end",   type=int, default=None, help="Last month in range")
    args = parser.parse_args()

    #Make the month range from arguments
    if args.month_start and args.month_end:
        if(args.month_start > args.month_end):
            print(f"ERROR: Month start range ({args.month_start}) is greater than month end range({args.month_end})")
            exit(1)

        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)

    print(f"\nNYC Taxi Silver Transformation")
    print(f"Year: {args.year} | Months: {months}")

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