"""
NOAA Weather Data Bronze Ingestion
=========================
Extracts the csv files from each year tar.gz file and read into seperate
pyarrow tables which are then concatenated into one ensuring columns are
casted to the right types if they change

Each year is appended as a new batch, creating a new Delta version
that can be queried independently via time travel.
 
Input:  data/raw/weather/{year}.tar.gz
Output: s3://bronze/weather/ (Delta table in MinIO)
 
Usage:
    python ingestion/weather/ingestion.py                                    # Ingests 2023 by default
    python ingestion/weather/ingestion.py --year 2022                        # Ingests specific year
    python ingestion/weather/ingestion.py --year-start 2020 --year-end 2023  # Ingests a range
"""

import tarfile
import tempfile
import argparse
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

INPUT_DIR       = Path("data/raw/weather")
DELTA_TABLE_URI = "s3://bronze/weather"

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

def build_source_path(year: int) -> Path:
    return INPUT_DIR / f"{year}.tar.gz"

def extract_data_to_table(file: Path) -> pa.Table:
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Extract all CSVs into temp directory
        with tarfile.open(file, "r:gz") as tar:
            tar.extractall(tmp_dir, filter='data')

        # Read and concatenate all CSVs into a single PyArrow table
        tables = []
        for csv_file in Path(tmp_dir).glob("*.csv"):
            table = pa_csv.read_csv(csv_file)

            # Cast STATION to string to ensure consistency across all CSVs
            idx = table.schema.get_field_index("STATION")
            if idx != -1:
                table = table.set_column(idx, "STATION", pc.cast(table["STATION"], pa.string()))


            tables.append(table)

        return pa.concat_tables(tables, promote_options="default")

def add_audit_columns(table: pa.Table, source_file: str, year : int) -> pa.Table:
    num_rows = table.num_rows
    now = datetime.now(timezone.utc)
 
    return (
        table
        .append_column("ingested_at",  pa.array([now] * num_rows, type=pa.timestamp("us", tz="UTC")))
        .append_column("source_file",  pa.array([source_file] * num_rows, type=pa.string()))
        .append_column("source_year",  pa.array([year] * num_rows, type=pa.int16()))
        .append_column("source_month", pc.cast(pc.month(table["DATE"]), pa.int8()))
    )

def table_exists() -> bool:
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
    parser = argparse.ArgumentParser(description="Ingest NOAA GSOD weather archive files")
    parser.add_argument("--year",       type=int, default=2023, help="Single year to ingest (default: 2023)")
    parser.add_argument("--year-start", type=int, default=None, help="First year in range")
    parser.add_argument("--year-end",   type=int, default=None, help="Last year in range")

    args = parser.parse_args()

    # Build the year range
    if args.year_start and args.year_end:
        if args.year_start > args.year_end:
            print(f"ERROR: Year start ({args.year_start}) is greater than year end ({args.year_end})")
            exit(1)
        years = range(args.year_start, args.year_end + 1)
    else:
        #Default to just the given year if no range given
        years = [args.year]

    for year in years:
        source_path = build_source_path(year)

        #Check if the files have already been downloaded before being ingested into minio
        if not source_path.exists():
            print(f"  ERROR: Source file not found: {source_path}")
            print(f"  Run download.py first for {year} NOAA Weather Data \n")
            continue

        print(f"\nBeginning ingestion of {year} NOAA Weather Data...")

        try:
            print(f"Extracting data from {source_path} into one table...")
            weather_data = extract_data_to_table(source_path)
            row_count   = weather_data.num_rows
            print(f"Rows read: {row_count:,}")
 
            # Add audit columns before writing, this will be used for time travel queries
            print("Adding audit columns...")
            weather_data = add_audit_columns(weather_data, source_path.name, year)


            print("Writing data to Delta Lake in MinIO...")
            if table_exists():
                write_deltalake(
                    DELTA_TABLE_URI,
                    weather_data,
                    mode = 'overwrite',
                    predicate=f"source_year = {year}", #Deletes and rewrites all partitions for this year
                    schema_mode="merge", 
                    storage_options= STORAGE_OPTIONS,
                )
            else:
                write_deltalake(
                    DELTA_TABLE_URI,
                    weather_data,
                    mode = 'overwrite',
                    partition_by=["source_year", "source_month"],
                    schema_mode="overwrite",
                    storage_options= STORAGE_OPTIONS,
                )

            print(f"Successfully ingested {row_count:,} rows for {year}  NOAA Weather Data \n")
        except Exception as e:
            print(f"ERROR: Failed to ingest {year}  NOAA Weather Data: {e} \n")
            

if __name__ == "__main__":
    main()