"""
Weather Gold Ingestion
======================
Reads Gold view Parquet files that dbt built and writes them
as Delta tables to MinIO under s3://gold/weather/.

The NOAA dataset is static, so tables are fully overwritten.
No partitioning is applied.

Must run AFTER: dbt build --select tag:weather

Usage:
    python ingestion/weather/gold_ingestion.py
"""

import os
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
    "monthly_climate": {
        "source_uri": "s3://artifacts/dbt/weather/staging/monthly_climate.parquet",
        "write_uri": "s3://gold/weather/monthly_climate",
    },
    "station_metrics": {
        "source_uri": "s3://artifacts/dbt/weather/staging/station_metrics.parquet",
        "write_uri": "s3://gold/weather/station_metrics",
    },
    "daily_extremes": {
        "source_uri": "s3://artifacts/dbt/weather/staging/daily_extremes.parquet",
        "write_uri": "s3://gold/weather/daily_extremes",
    },
}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
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

def ingest_model(model_name: str, model_details: dict) -> bool:
    print(f"\n  [{model_name}]")
    print(f"  Reading from : {model_details['source_uri']}")
    print(f"  Writing to   : {model_details['write_uri']}")

    try:
        gold_table = read_parquet_from_minio(model_details["source_uri"])
        row_count = gold_table.num_rows
        print(f"  Rows         : {row_count:,}")

        if row_count == 0:
            print(f"  [WARN] View returned 0 rows — skipping Delta write")
            return False

        # Overwrite the entire Delta table (static dataset)
        write_deltalake(
            model_details["write_uri"],
            gold_table,
            mode          = "overwrite",
            schema_mode   = "overwrite",
            storage_options = STORAGE_OPTIONS,
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
    print("Weather Gold Ingestion")
    for model_name, model_details in GOLD_MODELS.items():
        ingest_model(model_name, model_details)

if __name__ == "__main__":
    main()