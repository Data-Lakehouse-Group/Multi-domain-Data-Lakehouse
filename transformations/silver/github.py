"""
GitHub Archive Silver Transformation
====================================
Reads from the Bronze Delta table (raw JSON strings), parses the
flattened JSON fields, enriches with datetime components, performs
basic cleaning, and writes a clean Silver Parquet file to MinIO.

Because the GH Archive dataset is static, we write a single
non-partitioned Silver output.  If you later ingest additional
hours, this script should be re-run to regenerate the entire Silver
file (it overwrites the existing Silver object).

Input:  s3://bronze/github_archive/
Output: s3://silver/github/github_silver.parquet

Usage:
    python transformations/silver/github.py
"""

import os
import sys
import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BRONZE_URI = "s3://bronze/github_archive"
SILVER_URI = "s3://silver/github/github_silver.parquet"

STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : os.getenv("AWS_REGION", "us-east-1"),
    "aws_s3_allow_unsafe_rename": "true",
}

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def read_bronze() -> pa.Table:
    """Read all data from the Bronze GitHub Archive Delta table."""
    dt = DeltaTable(BRONZE_URI, storage_options=STORAGE_OPTIONS)
    return dt.to_pyarrow_table()   # no filters – read everything

def table_exists(uri: str) -> bool:
    try:
        DeltaTable(uri, storage_options=STORAGE_OPTIONS)
        return True
    except Exception:
        return False

# ---------------------------------------------------------------------------
# TRANSFORMATION
# ---------------------------------------------------------------------------

def transform_bronze_to_silver(bronze_table: pa.Table) -> pa.Table:
    """
    Parse the JSON string columns (actor, repo, org, payload),
    extract key fields, and add time-based derived columns.
    """
    con = duckdb.connect()
    con.register("bronze_github", bronze_table)

    # ---------------------------------------------------------------
    # The bronze schema:
    # id, type, actor (JSON str), repo (JSON str), org (JSON str),
    # payload (JSON str), public, created_at (datetime str),
    # ingested_at, source_file, source_year, source_month, source_day, source_hour
    # ---------------------------------------------------------------

    result = con.execute("""
        SELECT
            -- Core fields
            id,
            type AS event_type,
            CAST(public AS BOOLEAN) AS public,
            CAST(created_at AS TIMESTAMP) AS created_at,

            -- Parse actor JSON
            json_extract_string(actor, '$.login') AS actor_login,
            json_extract_string(actor, '$.id') AS actor_id,

            -- Parse repo JSON (usually { "id": ..., "name": "owner/repo" })
            json_extract_string(repo, '$.name') AS repo_name,
            json_extract_string(repo, '$.id') AS repo_id,

            -- Parse org JSON (may be null for many events)
            CASE
                WHEN org IS NOT NULL AND json_valid(org)
                THEN json_extract_string(org, '$.login')
                ELSE NULL
            END AS org_login,

            -- Parse payload – keep as string for flexibility, but extract common fields
            payload AS payload_raw,

            -- Time components
            YEAR(created_at)   AS year,
            MONTH(created_at)  AS month,
            DAY(created_at)    AS day,
            HOUR(created_at)   AS hour,
            DAYOFWEEK(created_at) AS day_of_week,
            CAST(created_at AS TIME) AS event_time,

            -- Carry forward source file
            source_file
        FROM bronze_github
        WHERE
            -- Basic cleaning: drop rows without a valid id or type
            id IS NOT NULL
            AND type IS NOT NULL
            AND created_at IS NOT NULL
    """).fetch_arrow_table()

    return result

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    print("GitHub Archive Silver Transformation")
    print(f"Reading Bronze from {BRONZE_URI}...")
    try:
        bronze_data = read_bronze()
    except Exception as e:
        print(f"ERROR reading Bronze table: {e}")
        sys.exit(1)

    print(f"Read {bronze_data.num_rows:,} rows from Bronze")

    print("Transforming data...")
    silver_data = transform_bronze_to_silver(bronze_data)
    print(f"Silver rows: {silver_data.num_rows:,}")

    print(f"Writing to Silver: {SILVER_URI}")
    try:
        # Overwrite the entire Silver Parquet (GH Archive is static)
        write_deltalake(
            SILVER_URI,
            silver_data,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=STORAGE_OPTIONS,
        )
        print("Silver write complete.")
    except Exception as e:
        print(f"ERROR writing Silver: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()