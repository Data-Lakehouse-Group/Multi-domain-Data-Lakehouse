"""
GitHub Archive Bronze Ingestion
=========================
Reads raw json.gz files from GH Archive, extracts relevant fields 
and writes to a Delta Lake table in MinIO.
 
The json is flattened and stored as strings in their field and 
the file is partitioned under year, month, day and hour
 
Input:  data/raw/github_archive/{year}-{month}-{day}-{hour}.json.gz
Output: s3://bronze/github_archive/ (Delta table in MinIO)
 
Usage:
   python ingestion/github_archive/ingestion.py                                                               # Default Ingests all of 2023-02-01-1, Hour 1 of 2nd Feb 2023
   python ingestion/github_archive/ingestion.py --date-hour 2024-01-01-2                                      # Ingests all of 2023-02-01-2, Hour 1 of 2nd Feb 2023
   python ingestion/github_archive/ingestion.py --date-hour-start 2024-01-01-1 --date-hour-end 2024-01-02-2   # Range of hours to ingest between days
"""

import gzip
import json
import argparse

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pyarrow as pa

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

INPUT_DIR       = Path("data/raw/github_archive")
DELTA_TABLE_URI = "s3://bronze/github_archive"

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

def build_source_path(target_date: datetime) -> Path:
    return INPUT_DIR / f"{target_date.year}-{target_date.month:02d}-{target_date.day:02d}-{target_date.hour}.json.gz"

def read_file_into_table(file: Path) -> pa.Table:
    rows = {
        "id":         [],
        "type":       [],
        "actor":      [],
        "repo":       [],
        "org":        [],
        "payload":    [],
        "public":     [],
        "created_at": [],
    }

    with gzip.open(file, 'rt', encoding='utf-8') as f:
        for line in f:
            event = json.loads(line)
            rows["id"].append(event.get("id"))
            rows["type"].append(event.get("type"))
            rows["actor"].append(json.dumps(event.get("actor")))
            rows["repo"].append(json.dumps(event.get("repo")))
            rows["org"].append(json.dumps(event.get("org")))
            rows["payload"].append(json.dumps(event.get("payload")))
            rows["public"].append(event.get("public"))
            rows["created_at"].append(event.get("created_at"))

    return pa.table({
        "id":         pa.array(rows["id"],         type=pa.string()),
        "type":       pa.array(rows["type"],        type=pa.string()),
        "actor":      pa.array(rows["actor"],       type=pa.string()),
        "repo":       pa.array(rows["repo"],        type=pa.string()),
        "org":        pa.array(rows["org"],         type=pa.string()),
        "payload":    pa.array(rows["payload"],     type=pa.string()),
        "public":     pa.array(rows["public"],      type=pa.bool_()),
        "created_at": pa.array(rows["created_at"],  type=pa.string()),
    })

def add_audit_columns(table: pa.Table, source_file: str,year: int, month: int, day : int, hour: int) -> pa.Table:
    num_rows = table.num_rows
    now = datetime.now(timezone.utc)
 
    return (
        table
        .append_column("ingested_at",  pa.array([now] * num_rows, type=pa.timestamp("us", tz="UTC")))
        .append_column("source_file",  pa.array([source_file] * num_rows, type=pa.string()))
        .append_column("source_year",  pa.array([year] * num_rows, type=pa.int16()))
        .append_column("source_month", pa.array([month] * num_rows, type=pa.int8()))
        .append_column("source_day",  pa.array([day] * num_rows, type=pa.int8()))
        .append_column("source_hour", pa.array([hour] * num_rows, type=pa.int8()))
    )

def table_exists() -> bool:
    try:
        DeltaTable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS)
        return True
    except TableNotFoundError:
        return False
    
def parse_date_hour(date_str: str) -> datetime:
    dt = datetime.strptime(date_str, "%Y-%m-%d-%H")
    return dt
# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    #Sets up the parser for the CLI call of this file
    parser = argparse.ArgumentParser(description="Ingest GH Archive hourly JSON.GZ files")
    parser.add_argument("--date-hour",       type=str, default='2023-02-01-1', help="Single hour for day  (YYYY-MM-DD-HR)")
    parser.add_argument("--date-hour-start", type=str, default=None, help="Start of date hour range (YYYY-MM-DD-HR)")
    parser.add_argument("--date-hour-end",   type=str, default=None, help="End of date hour range   (YYYY-MM-DD-HR)")

    args = parser.parse_args()

    # Build the hour range
    if args.date_hour_start and args.date_hour_end:
        date_hour_start = parse_date_hour(args.date_hour_start)
        date_hour_end = parse_date_hour(args.date_hour_end)
        
        if date_hour_start > date_hour_end:
            print(f"ERROR: date-hour-start ({args.date_hour_start}) is after date-hour-end ({args.date_hour_end})")
            exit(1)
 
        datetimes = [date_hour_start + timedelta(hours=i) 
                        for i in range(int((date_hour_end - date_hour_start).total_seconds() // 3600) + 1)]
    else:
        datetimes = [parse_date_hour(args.date_hour)]

    for dt in datetimes:
        source_path = build_source_path(dt)

        #Check if the files have already been downloaded before being ingested into minio
        if not source_path.exists():
            print(f"  ERROR: Source file not found: {source_path}")
            print(f"  Run download.py first for the day and hour {dt} for Github Archive Data \n")
            continue

        print(f"\nBeginning ingestion for the day and hour {dt} for Github Archive Data...")

        try:
            print(f"Extracting data from {source_path} into table...")
            gh_archive_data = read_file_into_table(source_path)

            row_count   = gh_archive_data.num_rows
            print(f"Rows read: {row_count:,}")
 
            # Add audit columns before writing, this will be used for time travel queries
            print("Adding audit columns...")
            gh_archive_data = add_audit_columns(gh_archive_data, source_path.name, dt.year, dt.month, dt.day, dt.hour) #This version adds hour and day to partition


            print("Writing data to Delta Lake in MinIO...")
            if table_exists():
                write_deltalake(
                    DELTA_TABLE_URI,
                    gh_archive_data,
                    mode = 'overwrite',
                    predicate=f"source_year = {dt.year} AND source_month = {dt.month} AND source_day = {dt.day} AND source_hour = {dt.hour}",
                    schema_mode="merge", 
                    storage_options= STORAGE_OPTIONS,
                )
            else:
                write_deltalake(
                    DELTA_TABLE_URI,
                    gh_archive_data,
                    mode = 'overwrite',
                    partition_by=["source_year", "source_month", "source_day", "source_hour"],
                    schema_mode="overwrite",
                    storage_options= STORAGE_OPTIONS,
                )

            print(f"Successfully ingested {row_count:,} rows for the day and hour {dt} for Github Archive Data \n")
        except Exception as e:
            print(f"ERROR: Failed to ingest for the day and hour {dt} for Github Archive Data: {e} \n")
        

if __name__ == "__main__":
    main()