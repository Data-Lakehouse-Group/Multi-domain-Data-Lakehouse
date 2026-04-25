"""
NYC Taxi Bronze Ingestion
=========================
Reads raw Parquet files from local staging and writes them into
a single Delta table in MinIO as-is. No transformations applied.
 
Each month is appended as a new batch, creating a new Delta version
that can be queried independently via time travel.
 
Input:  data/raw/taxi/yellow_tripdata_{year}-{month}.parquet
Output: s3://bronze/taxi/yellow_tripdata/ (Delta table in MinIO)
 
Usage: (add --debug at the end of each for local testing)
    python ingestion/taxi/bronze_ingestion.py                    # Ingests 2023 full year by default
    python ingestion/taxi/bronze_ingestion.py --year 2022        # Ingests specific year
    python ingestion/taxi/bronze_ingestion.py --year 2023 --month-start 1 --month-end 1   # Ingests January only for 2023
    python ingestion/taxi/bronze_ingestion.py --year 2023 --month-start 1 --month-end 5   # Ingests January to May for 2023
"""


import os
import argparse
import calendar
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SOURCE_URI    = "raw/taxi"
BRONZE_URI = "s3://bronze/taxi"

# MinIO connection
STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : "us-east-1",
    "aws_s3_allow_unsafe_rename": "true",
}
# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def get_s3_filesystem() -> pafs.S3FileSystem:
    endpoint = STORAGE_OPTIONS["endpoint_url"].replace("http://", "").replace("https://", "")
    scheme   = "https" if STORAGE_OPTIONS.get("aws_use_ssl", "false") == "true" else "http"

    return pafs.S3FileSystem(
        endpoint_override = endpoint,
        access_key        = STORAGE_OPTIONS["aws_access_key_id"],
        secret_key        = STORAGE_OPTIONS["aws_secret_access_key"],
        scheme            = scheme,
    )

def file_exists_in_minio(fs: pafs.S3FileSystem, path: str) -> bool:
    try:
        info = fs.get_file_info(path)
        return info.type != pafs.FileType.NotFound
    except Exception:
        return False

def read_source_file(fs: pafs.S3FileSystem ,year: int, month: int) -> pa.Table:
    filename = f"{year}-{month:02d}.parquet"
    path     = f"{SOURCE_URI}/{filename}"

    if not file_exists_in_minio(fs, path):
        raise FileNotFoundError(
            f"Source file not found: s3://{path}\n"
            f"Run download.py --year {year} --month-start {month} --month-end {month} first."
        )

    print(f"Reading from s3://{SOURCE_URI}")
    return pq.read_table(path, filesystem=fs)

def add_audit_columns(table: pa.Table, year : int, month: int) -> pa.Table:
    num_rows = table.num_rows
    now = datetime.now(timezone.utc)
    source_path = f"{SOURCE_URI}/{year}-{month:02d}.parquet"
 
    return (
        table
        .append_column("ingested_at",  pa.array([now] * num_rows, type=pa.timestamp("us", tz="UTC")))
        .append_column("source_file",  pa.array([source_path] * num_rows, type=pa.string()))
        .append_column("source_year",  pa.array([year] * num_rows, type=pa.int16()))
        .append_column("source_month", pa.array([month] * num_rows, type=pa.int8()))
    )

def table_exists(DELTA_TABLE_URI: str) -> bool:
    try:
        DeltaTable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS)
        return True
    except TableNotFoundError:
        return False
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
    if args.month_start is not None and args.month_end is not None:
        if(args.month_start > args.month_end):
            print(f"ERROR: Month start range ({args.month_start}) is greater than month end range({args.month_end})")
            exit(1)

        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)
    
    year = args.year

    fs = get_s3_filesystem() #Gets the MinIO (S3) file configs

    for month in months:
        month_name = calendar.month_name[month]

        print(f"\nBeginning ingestion of {month_name} {year} NYC taxi data")

        try:
            #Check if the files have already been downloaded before being ingested into minio
            taxi_data = read_source_file(year, month)

            row_count   = taxi_data.num_rows
            print(f"Rows read: {row_count:,}")

            print(f"Adding audit columns...")
            # Add audit columns before writing, this will be used for time travel queries
            taxi_data = add_audit_columns(taxi_data, year, month)
            print(f"Successfully added audit columns")

            print("Writing data to Delta Lake in MinIO...")
            if table_exists(BRONZE_URI):
                write_deltalake(
                    BRONZE_URI,
                    taxi_data,
                    mode = 'overwrite',
                    predicate=f"source_year = {year} AND source_month = {month}",
                    schema_mode="merge", 
                    storage_options= STORAGE_OPTIONS,
                )
            else:
                write_deltalake(
                    BRONZE_URI,
                    taxi_data,
                    mode = 'overwrite',
                    partition_by=["source_year", "source_month"],
                    schema_mode="overwrite",
                    storage_options= STORAGE_OPTIONS,
                )

            print(f"[OK] Successfully ingested {row_count:,} rows for {month_name} {year} NYC Taxi Dataset \n")
        except Exception as e:
            print(f"[ERROR]: Failed to ingest {month_name} {year} NYC taxi data: {e} \n")
            

if __name__ == "__main__":
    main()