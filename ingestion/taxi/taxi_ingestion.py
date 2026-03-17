"""
NYC Taxi Bronze Ingestion
=========================
Reads raw Parquet files from local staging and writes them into
a single Delta table in MinIO as-is. No transformations applied.
 
Each month is appended as a new batch, creating a new Delta version
that can be queried independently via time travel.
 
Input:  data/raw/taxi/yellow_tripdata_{year}-{month}.parquet
Output: s3://bronze/taxi/yellow_tripdata/ (Delta table in MinIO)
 
Usage:
    python download.py                    # Ingests 2023 full year by default
    python download.py --year 2022        # Ingests specific year
    python download.py --year 2023 --month-start 1 --month-end 1   # Ingests January only for 2023
    python download.py --year 2023 --month-start 1 --month-end 5   # Ingests January to May for 2023
"""


import argparse
import calendar
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

INPUT_DIR       = Path("data/raw/taxi")
DELTA_TABLE_URI = "s3://bronze/taxi/yellow_tripdata"

# MinIO connection — must match your docker-compose.yml
STORAGE_OPTIONS = {
    "endpoint_url"       : "http://localhost:9000",
    "aws_access_key_id"  : "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name"        : "us-east-1",
    "allow_http"         : "true",          # Required for non-HTTPS MinIO
    "aws_s3_allow_unsafe_rename": "true",   # Required for Delta Lake on MinIO
}

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def build_source_path(year: int, month: int) -> Path:
    return INPUT_DIR / f"yellow_tripdata_{year}-{month:02d}.parquet"

def add_audit_columns(table: pa.Table, source_file: str) -> pa.Table:
    num_rows = table.num_rows
    now = datetime.now(timezone.utc).isoformat()
 
    ingested_at_col = pa.array([now] * num_rows, type=pa.string())
    source_file_col = pa.array([source_file] * num_rows, type=pa.string())
 
    table = table.append_column("ingested_at", ingested_at_col)
    table = table.append_column("source_file", source_file_col)
 
    return table

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    #Sets up the parser for the CLI call of this file
    parser = argparse.ArgumentParser(description="Ingest NYC Taxi Data into Bronze Delta Tables")
    parser.add_argument("--year",  type=int, default=2023, help="Year to download (default: 2023)")
    parser.add_argument("--month-start", type=int, default=None, help="First month in range. Omit for full year")
    parser.add_argument("--month-end", type=int, default=None, help="Last month in range. Omit for full year")

    args = parser.parse_args()

    #Make the month range from arguments
    if args.month_start and args.month_end:
        if(args.month_start > args.month_end):
            print(f"ERROR: Month start range ({args.month_start}) is greater than month end range({args.month_end})")
            exit(1)

        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)
    
    year = args.year

    for month in months:
        source_path = build_source_path(year, month)
        month_name = calendar.month_name[month]

        #Check if the files have already been downloaded before being ingested into minio
        if not source_path.exists():
            print(f"  ERROR: Source file not found: {source_path}")
            print(f"  Run download.py first for {year}-{month:02d}")
            continue

        print(f"\nBeginning ingestion of {month_name} {year} NYC taxi data")

        try:
            taxi_data = pq.read_table(source_path)
            row_count   = taxi_data.num_rows
            print(f"Rows read: {row_count:,}")
 
            # Add audit columns before writing, this will be used for time travel queries
            taxi_data = add_audit_columns(taxi_data, source_path.name)

            #Check if the table was already ingested and set write mode accordingly
            try:
                DeltaTable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS)
                writing_mode = "append"
            except:
                writing_mode = "overwrite"

            write_deltalake(
                DELTA_TABLE_URI,
                taxi_data,
                mode = writing_mode,
                storage_options= STORAGE_OPTIONS,
            )

            print(f"Successfully ingested {row_count:,} rows for {month_name} {year} NYC Taxi Dataset \n")
        except Exception as e:
            print(f"ERROR: Failed to ingest {month_name} {year} NYC taxi data: {e} \n")
            

if __name__ == "__main__":
    main()