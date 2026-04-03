"""Bronze: write all 2024 months to a single Delta table (partitioned by year/month)"""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError

INPUT_DIR = Path("data/raw/taxi")
DELTA_TABLE_URI = "s3://bronze/taxi/yellow_tripdata"
STORAGE_OPTIONS = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

def add_audit_columns(table: pa.Table, source_file: str, year: int, month: int) -> pa.Table:
    now = datetime.now(timezone.utc)
    num_rows = table.num_rows
    return (table
        .append_column("ingested_at", pa.array([now] * num_rows, type=pa.timestamp("us", tz="UTC")))
        .append_column("source_file", pa.array([source_file] * num_rows))
        .append_column("source_year", pa.array([year] * num_rows, type=pa.int16()))
        .append_column("source_month", pa.array([month] * num_rows, type=pa.int8()))
    )

def table_exists():
    try:
        DeltaTable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS)
        return True
    except TableNotFoundError:
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month-start", type=int, default=1)
    parser.add_argument("--month-end", type=int, default=12)
    args = parser.parse_args()

    for month in range(args.month_start, args.month_end + 1):
        source_path = INPUT_DIR / f"yellow_tripdata_{args.year}-{month:02d}.parquet"
        if not source_path.exists():
            print(f"Missing {source_path}, run download.py first")
            continue

        print(f"Processing {args.year}-{month:02d} ...")
        df = pq.read_table(source_path)
        df = add_audit_columns(df, source_path.name, args.year, month)

        if table_exists():
            write_deltalake(
                DELTA_TABLE_URI, df, mode="overwrite",
                predicate=f"source_year = {args.year} AND source_month = {month}",
                schema_mode="merge", storage_options=STORAGE_OPTIONS
            )
        else:
            write_deltalake(
                DELTA_TABLE_URI, df, mode="overwrite",
                partition_by=["source_year", "source_month"],
                schema_mode="overwrite", storage_options=STORAGE_OPTIONS
            )
        print(f"  Ingested {df.num_rows:,} rows")

if __name__ == "__main__":
    main()