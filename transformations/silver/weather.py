"""
NOAA Weather Silver Transformation
==================================
Reads from the Bronze Delta table (raw GSOD CSVs), applies cleaning,
derives categorical columns (season, is_freezing, has_precipitation),
and writes to a single Silver Parquet file in MinIO.

Because the dataset is large but static, we write a non-partitioned
Silver output that overwrites the previous version.

Input:  s3://bronze/weather/
Output: s3://silver/weather/gsod_silver.parquet

Usage:
    python transformations/silver/weather.py
"""

import os
import sys
import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BRONZE_URI = "s3://bronze/weather"
SILVER_URI  = "s3://silver/weather/gsod_silver.parquet"

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
    """Read all Bronze weather data (all years)."""
    dt = DeltaTable(BRONZE_URI, storage_options=STORAGE_OPTIONS)
    return dt.to_pyarrow_table()

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
    Clean and enrich:
    - Drop rows with missing critical fields
    - Cast data types
    - Derive season, is_freezing, has_precipitation
    """
    con = duckdb.connect()
    con.register("bronze_weather", bronze_table)

    result = con.execute("""
        SELECT
            -- Keep original fields with proper types
            CAST(STATION AS VARCHAR) AS STATION,
            CAST(DATE AS DATE) AS DATE,
            CAST(LATITUDE AS DOUBLE) AS LATITUDE,
            CAST(LONGITUDE AS DOUBLE) AS LONGITUDE,
            CAST(ELEVATION AS DOUBLE) AS ELEVATION,
            CAST(NAME AS VARCHAR) AS NAME,

            -- Core weather metrics (handling missing values coded as 9999.9)
            CASE WHEN TEMP > 999 THEN NULL ELSE CAST(TEMP AS DOUBLE) END AS TEMP,
            CASE WHEN DEWP > 999 THEN NULL ELSE CAST(DEWP AS DOUBLE) END AS DEWP,
            CASE WHEN SLP  > 9999 THEN NULL ELSE CAST(SLP AS DOUBLE) END AS SLP,
            CASE WHEN STP  > 9999 THEN NULL ELSE CAST(STP AS DOUBLE) END AS STP,
            CASE WHEN VISIB > 999 THEN NULL ELSE CAST(VISIB AS DOUBLE) END AS VISIB,
            CASE WHEN WDSP > 999 THEN NULL ELSE CAST(WDSP AS DOUBLE) END AS WDSP,
            CASE WHEN MXSPD > 999 THEN NULL ELSE CAST(MXSPD AS DOUBLE) END AS MXSPD,
            CASE WHEN GUST > 999 THEN NULL ELSE CAST(GUST AS DOUBLE) END AS GUST,
            CASE WHEN MAX > 999 THEN NULL ELSE CAST(MAX AS DOUBLE) END AS MAX,
            CASE WHEN MIN > 999 THEN NULL ELSE CAST(MIN AS DOUBLE) END AS MIN,
            CASE WHEN PRCP > 99.99 THEN NULL ELSE CAST(PRCP AS DOUBLE) END AS PRCP,
            CASE WHEN SNDP > 999.9 THEN NULL ELSE CAST(SNDP AS DOUBLE) END AS SNDP,
            CAST(FRSHTT AS VARCHAR) AS FRSHTT,

            -- Derived columns
            YEAR(DATE)  AS year,
            MONTH(DATE) AS month,
            DAYOFWEEK(DATE) AS day_of_week,

            CASE
                WHEN MONTH(DATE) IN (12, 1, 2) THEN 'winter'
                WHEN MONTH(DATE) IN (3, 4, 5)  THEN 'spring'
                WHEN MONTH(DATE) IN (6, 7, 8)  THEN 'summer'
                WHEN MONTH(DATE) IN (9, 10, 11) THEN 'fall'
            END AS season,

            CASE WHEN TEMP <= 0 THEN 1 ELSE 0 END AS is_freezing,
            CASE WHEN PRCP > 0 THEN 1 ELSE 0 END AS has_precipitation

        FROM bronze_weather
        WHERE
            -- Keep only rows with a valid station and date
            STATION IS NOT NULL
            AND DATE IS NOT NULL
            -- Remove extreme placeholder latitudes (often 0 or 999)
            AND LATITUDE IS NOT NULL
            AND LATITUDE BETWEEN -90 AND 90
            AND LONGITUDE IS NOT NULL
            AND LONGITUDE BETWEEN -180 AND 180
    """).fetch_arrow_table()

    return result

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    print("NOAA Weather Silver Transformation")
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