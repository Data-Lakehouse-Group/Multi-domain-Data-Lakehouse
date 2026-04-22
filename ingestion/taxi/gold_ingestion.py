"""
Taxi Gold Ingestion
====================
Reads Gold views that dbt built in the persistent DuckDB file
and writes them as Delta tables to MinIO.

This file facilitates a seperation of concerns where dbt only
handles data aggregation logic while the gold_ingestion.py
handles the write to MinIO as a Delta Table

Must run AFTER: dbt run --select tag:taxi

Usage:
    python ingestion/taxi/gold_ingestion.py
    python ingestion/taxi/gold_ingestion.py --model daily_summary
"""

import os
import argparse
import calendar
from deltalake import write_deltalake, DeltaTable
import duckdb


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./tmp/lakehouse.duckdb")

STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : "us-east-1",
    "aws_s3_allow_unsafe_rename": "true",
}

# All Gold Aggregations which exist in the project
GOLD_MODELS = {
    "daily_summary": {
        "view"        : "daily_summary",
        "uri"         : "s3://gold/taxi/yellow_tripdata/daily_summary"
    },
    "zone_performance": {
        "view"        : "zone_performance",
        "uri"         : "s3://gold/taxi/yellow_tripdata/zone_performance"
    },
    "individual_day_summary": {
        "view"        : "individual_day_summary",
        "uri"         : "s3://gold/taxi/yellow_tripdata/individual_day_summary"
    },
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

def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DUCKDB_PATH, read_only=False)  # needs write for httpfs setup
    
    endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000").replace("http://", "")

    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL delta;
        LOAD delta;
        SET s3_endpoint          = '{endpoint}';
        SET s3_access_key_id     = '{os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")}';
        SET s3_secret_access_key = '{os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")}';
        SET s3_use_ssl           = false;
        SET s3_url_style         = 'path';
        SET s3_region            = 'us-east-1';
    """)
    return con

# ---------------------------------------------------------------------------
# CORE FUNCTIONS
# ---------------------------------------------------------------------------
def ingest_model(
        con: duckdb.DuckDBPyConnection, 
        model_name: str, 
        model_details: dict, 
        year: int, 
        month: int
    ) -> bool:

    print(f"\n  [{model_name}]")
    print(f"  Reading view : {model_details['view']}")
    print(f"  Writing to   : {model_details['uri']}")

    try:
        #Read into an arrow table for write to delta table
        gold_table = con.execute(f"SELECT * FROM {model_details['view']}").fetch_arrow_table()

        row_count = gold_table.num_rows
        print(f"  Rows         : {row_count:,}")

        if row_count == 0:
            print(f"  [WARN] View returned 0 rows — skipping Delta write")
            return False

        if table_exists(model_details['uri']):
            write_deltalake(
                model_details['uri'],
                gold_table,
                mode = 'overwrite',
                predicate=f"source_year = {year} AND source_month = {month}",
                schema_mode="merge", 
                storage_options= STORAGE_OPTIONS,
            )
        else:
            write_deltalake(
                model_details['uri'],
                gold_table,
                mode = 'overwrite',
                partition_by=["source_year", "source_month"],
                schema_mode="overwrite",
                storage_options= STORAGE_OPTIONS,
            )

        print(f"  [OK]: Successfully ingeted {model_name}")
        return True

    except Exception as e:
        print(f"  [ERROR] Failed to ingest {model_name}: {e}")
        return False


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Write Taxi Gold dbt views as Delta tables to MinIO")
    parser.add_argument("--year", default=2023,  type=int, help="Partition year e.g. 2023")
    parser.add_argument("--month", default=1, type=int, help="Partition month e.g. 10")
    args = parser.parse_args()

    print(f"\nTaxi Gold Ingestion")
    print(f"DuckDB file : {DUCKDB_PATH}")
    print(f"Ingesting data aggregated from year: {args.year} month: {calendar.month_name[args.month]}")

    for model_name, model_details in GOLD_MODELS.items():
        #Connect to the temporary directory where queries are stored
        con = get_connection()

        ingest_model(
            con,
            model_name, 
            model_details, 
            args.year, 
            args.month
        )

        con.close() #Close the duckdb connection

if __name__ == "__main__":
    main()