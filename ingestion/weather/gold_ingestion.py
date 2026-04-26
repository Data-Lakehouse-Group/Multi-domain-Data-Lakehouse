"""
Weather Gold Ingestion
======================
Reads Gold views that dbt built in the persistent DuckDB file
and writes them as Delta tables to MinIO.

This file facilitates a separation of concerns where dbt only
handles data aggregation logic while the gold_ingestion.py
handles the write to MinIO as a Delta Table

Must run AFTER: dbt run --select tag:taxi

Usage:
    python ingestion/weather/gold_ingestion.py                                    #Defaults to 2023
    python ingestion/weather/gold_ingestion.py --year-start 2020 --year-end 2023  # Ingests a range
"""

import os
import argparse
import calendar
from deltalake import write_deltalake, DeltaTable
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : os.getenv("AWS_REGION", "us-east-1"),
    "aws_s3_allow_unsafe_rename": "true",
}

GOLD_MODELS = {
    "daily_climate_summary": {
        "view"        : "daily_climate_summary",
        "write_uri"   : "s3://gold/taxi/daily_climate_summary",
        "source_uri"  : "s3://artifacts/dbt/taxi/staging/daily_climate_summary.parquet"
    },
    "monthly_climates": {
        "view"        : "monthly_climates",
        "write_uri"   : "s3://gold/taxi/monthly_climates",
        "source_uri"  : "s3://artifacts/dbt/taxi/staging/monthly_climates.parquet"
    },
    "station_metrics": {
        "view"        : "station_metrics",
        "write_uri"   : "s3://gold/taxi/station_metrics",
        "source_uri"  : "s3://artifacts/dbt/taxi/staging/station_metrics.parquet"
    }
}

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------
def table_exists(uri: str) -> bool:
    try:
        DeltaTable(uri, storage_options=STORAGE_OPTIONS)
        return True
    except Exception:
        return False

def get_s3_filesystem() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(
        endpoint_override = STORAGE_OPTIONS["endpoint_url"].replace("http://", ""),
        access_key        = STORAGE_OPTIONS["aws_access_key_id"],
        secret_key        = STORAGE_OPTIONS["aws_secret_access_key"],
        scheme            = "http",
    )

def read_parquet_from_minio(uri: str) -> pa.Table:
    fs    = get_s3_filesystem()
    path  = uri.replace("s3://", "")
    return pq.read_table(path, filesystem=fs)

# ---------------------------------------------------------------------------
# CORE FUNCTIONS
# ---------------------------------------------------------------------------
def ingest_model(
        model_name: str, 
        model_details: dict, 
        year: int
    ) -> bool:

    print(f"\n  [{model_name}]")
    print(f"  Reading view : {model_details['view']}")
    print(f"  Reading from : {model_details['source_uri']}")
    print(f"  Writing to   : {model_details['write_uri']}")

    try:
        #Read parquet file into an arrow table for write to delta table
        gold_table = read_parquet_from_minio(model_details["source_uri"])

        row_count = gold_table.num_rows
        print(f"  Rows         : {row_count:,}")

        if row_count == 0:
            print(f"  [WARN] View returned 0 rows — skipping Delta write")
            return False

        if table_exists(model_details['write_uri']):
            write_deltalake(
                model_details['write_uri'],
                gold_table,
                mode = 'overwrite',
                predicate=f"source_year = {year} ",
                schema_mode="merge", 
                storage_options= STORAGE_OPTIONS,
            )
        else:
            write_deltalake(
                model_details['write_uri'],
                gold_table,
                mode = 'overwrite',
                partition_by=["source_year", "source_month"],
                schema_mode="overwrite",
                storage_options= STORAGE_OPTIONS,
            )

        print(f"  [OK]: Successfully ingested {model_name}")
        return True

    except Exception as e:
        print(f"  [ERROR] Failed to ingest {model_name}: {e}")
        return False

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    #Sets up the parser for the CLI call of this file
    parser = argparse.ArgumentParser(description="Ingest NOAA GSOD weather archive files")
    parser.add_argument("--year", type=int, default=2023, help="First year in range")
    args = parser.parse_args()

    print(f"\Weather Gold Ingestion")
    print(f"Ingesting data aggregated from year: {args.year} ")

    for model_name, model_details in GOLD_MODELS.items():
        ingest_model(
            model_name, 
            model_details, 
            args.year, 
        )

if __name__ == "__main__":
    main()