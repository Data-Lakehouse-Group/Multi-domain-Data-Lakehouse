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
    python transformations/silver/weather.py --year-start 2020 --year-end 2023  
"""

import os
import argparse

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BRONZE_URI = "s3://bronze/weather"
SILVER_URI  = "s3://silver/weather"

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
def table_exists(uri: str) -> bool:
    try:
        DeltaTable(uri, storage_options=STORAGE_OPTIONS)
        return True
    except Exception:
        return False

def read_bronze_table_for_year(year: int) -> pa.Table:
    dt = DeltaTable(BRONZE_URI, storage_options = STORAGE_OPTIONS)

    #Get the partitioned files on year and month
    arrow_table = dt.to_pyarrow_table(
        filters=[
            ("source_year",  "=", year)
        ],
    )

    return arrow_table

def print_new_columns(previous_table: pa.Table, enriched_table: pa.Table):
    previous_cols = set(previous_table.column_names)
    enriched_cols = set(enriched_table.column_names)

    new_columns = enriched_cols - previous_cols

    if new_columns:
        for col in sorted(new_columns):
            col_type = enriched_table.schema.field(col).type
            print(f"        {col} ({col_type})")
    else:
        print("\n       No new columns added during enrichment")


# ---------------------------------------------------------------------------
# TRANSFORMATION
# ---------------------------------------------------------------------------

def apply_quality_checks_and_enrich(
        con: duckdb.DuckDBPyConnection, 
        year: int
    ) -> pa.Table:

    result = con.execute(f"""
                         
        -- Step 1: Apply quality rules to bronze data, dropping any rows that fail
        WITH cleaned AS(
            SELECT *
            FROM bronze_weather
            WHERE
                -- Rule 1: No nulls on critical columns
                STATION   IS NOT NULL
                AND DATE  IS NOT NULL
                AND TEMP  IS NOT NULL
                AND WDSP  IS NOT NULL
                AND VISIB IS NOT NULL
                AND PRCP  IS NOT NULL
                AND LATITUDE  IS NOT NULL
                AND LONGITUDE IS NOT NULL

                -- Rule 2: Sentinel removal
                AND CAST(TEMP  AS DOUBLE) < 9999.9
                AND CAST(WDSP  AS DOUBLE) < 999.9
                AND CAST(PRCP  AS DOUBLE) < 99.99
                AND CAST(VISIB AS DOUBLE) < 999.9
                AND (
                    source_month NOT IN (12, 1, 2)
                    OR (
                        source_month IN (12, 1, 2)
                        AND SNDP IS NOT NULL
                        AND CAST(SNDP AS DOUBLE) < 999.9
                    )
                )

                -- Rule 3: Geospatial validity
                AND CAST(LATITUDE  AS DOUBLE) BETWEEN -90  AND 90
                AND CAST(LONGITUDE AS DOUBLE) BETWEEN -180 AND 180
                AND CAST(ELEVATION AS DOUBLE) BETWEEN -500 AND 9000

                -- Rule 4: Physical plausibility
                AND CAST(TEMP  AS DOUBLE) BETWEEN -129 AND 135
                AND CAST(WDSP  AS DOUBLE) BETWEEN 0   AND 200
                AND CAST(PRCP  AS DOUBLE) BETWEEN 0   AND 6
                AND CAST(VISIB AS DOUBLE) BETWEEN 0   AND 999

                -- Rule 5: Cross-field consistency
                AND (DEWP IS NULL OR CAST(DEWP AS DOUBLE) <= CAST(TEMP AS DOUBLE))
                AND (MIN  IS NULL OR MAX IS NULL OR CAST(MIN AS DOUBLE) <= CAST(MAX AS DOUBLE))
                AND (MIN  IS NULL OR MAX IS NULL OR TEMP IS NULL
                    OR CAST(TEMP AS DOUBLE) BETWEEN CAST(MIN AS DOUBLE) AND CAST(MAX AS DOUBLE))

                -- Rule 6: Date is correct
                AND CAST(DATE AS DATE) <= CURRENT_DATE
                         
                -- Rule 7: Record must belong to the correct year 
                AND YEAR(DATE)  = {year}
        )
                         
        -- Step 2: Add all remaining derived and audit columns
        SELECT
            -- Original columns with optimised storage types
            CAST(STATION   AS VARCHAR) AS STATION,
            CAST(DATE      AS DATE)    AS DATE,
            CAST(LATITUDE  AS FLOAT)  AS LATITUDE,
            CAST(LONGITUDE AS FLOAT)  AS LONGITUDE,
            CAST(ELEVATION AS SMALLINT)  AS ELEVATION,
            CAST(NAME      AS VARCHAR) AS NAME,

            CAST(TEMP      AS FLOAT)    AS TEMP,        
            CAST(DEWP      AS FLOAT)    AS DEWP,
            CAST(SLP       AS FLOAT)    AS SLP,
            CAST(STP       AS FLOAT)    AS STP,
            CAST(VISIB     AS FLOAT)    AS VISIB,
            CAST(WDSP      AS FLOAT)    AS WDSP,
            CAST(MXSPD     AS FLOAT)    AS MXSPD,
            CAST(GUST      AS FLOAT)    AS GUST,
            CAST(MAX       AS FLOAT)    AS MAX,
            CAST(MIN       AS FLOAT)    AS MIN,
            CAST(PRCP      AS FLOAT)    AS PRCP,
            CAST(SNDP      AS FLOAT)    AS SNDP,

            --Audit Colums
            source_year,
            source_month,
            source_file,
            ingested_at,            
                         
            -- Column 1: Seasons (Norther Hemisphere)
            CASE
                WHEN MONTH(DATE) IN (12, 1, 2)  THEN 'winter'
                WHEN MONTH(DATE) IN (3, 4, 5)   THEN 'spring'
                WHEN MONTH(DATE) IN (6, 7, 8)   THEN 'summer'
                WHEN MONTH(DATE) IN (9, 10, 11) THEN 'fall'
            END AS season_northern,             

            -- Column 2: Day of week
            DAYNAME(DATE) AS day_name,     

            -- Column 3: Is weekend
            CASE
                WHEN DAYOFWEEK(DATE) IN (1, 7) THEN TRUE ELSE FALSE
            END AS is_weekend,

            -- Column 4: Temp as Celsius
            ROUND((CAST(TEMP AS DOUBLE) - 32) * 5.0 / 9.0, 2) AS temp_c,

            -- Column 5: Max temp as Celsius
            ROUND((CAST(MAX AS DOUBLE) - 32) * 5.0 / 9.0, 2) AS max_temp_c,

            -- Column 6: Min temp as Celsius
            ROUND((CAST(MIN AS DOUBLE) - 32) * 5.0 / 9.0, 2) AS min_temp_c,

            -- Column 7: Wind speed knots → km/h
            ROUND(CAST(WDSP AS DOUBLE) * 1.852, 2)               AS wdsp_kmh,

            -- Column 8: Max wind speed knots → km/h
            ROUND(CAST(MXSPD AS DOUBLE) * 1.852, 2)              AS mxspd_kmh,

            -- Column 9: Gust knots → km/h
            ROUND(CAST(GUST AS DOUBLE) * 1.852, 2)               AS gust_kmh,
                         
            -- Column 10: Is freezing (temp at or below 32°F)
            CAST(TEMP AS DOUBLE) <= 32.0                            AS is_freezing,

            -- Column 11: Is extreme heat (temp at or above 100°F)
            CAST(TEMP AS DOUBLE) >= 100.0                           AS is_extreme_heat,

            -- Column 12: Has precipitation
            CAST(PRCP AS DOUBLE) > 0.0                             AS has_precipitation,

            -- Column 13: Is gale force wind (≥34 knots, Beaufort 8)
            CAST(WDSP AS DOUBLE) >= 34.0                          AS is_gale,
         
            -- Column 14: Audit timestamp for when this was processed at silver layer
            CURRENT_TIMESTAMP AS processed_at,

        FROM cleaned
    """).fetch_arrow_table()

    return result

# ---------------------------------------------------------------------------
# CORE TRANSFORMATION FUNCTION
# ---------------------------------------------------------------------------

def transform_year(year: int) -> bool:

    print(f"\n[SILVER] Transforming {year}")

    con = duckdb.connect()  # One connection for this year's entire lifecycle

    #Optimize memory and threads used
    con.execute("SET threads TO 4") 
    con.execute("SET memory_limit = '4GB'")
    
    try:
        #Step 1: Reading the appropriate year
        #partition from the Bronze Delta Table
        print(f"    Reading Bronze data for {year}...")
        bronze_pa_table = read_bronze_table_for_year(year)
        print(f"    Bronze rows read: {bronze_pa_table.num_rows:,}")

        #Step 2: Apply quality checks and feature engineering in a single pass
        print(f"    Applying quality checks and feature engineering...")
        con.register("bronze_weather", bronze_pa_table)
        silver_table = apply_quality_checks_and_enrich(con, year)

        print(f"    Rows dropped : {(bronze_pa_table.num_rows - silver_table.num_rows):,}")
        print(f"    Columns added:")
        print_new_columns(bronze_pa_table, silver_table)

        #Step 3: Write to Silver Delta Table
        print(f"\n   Writing to Silver Minio Bucket...")

        if table_exists(SILVER_URI):
            write_deltalake(
                SILVER_URI,
                silver_table,
                mode = 'overwrite',
                predicate=f"source_year = {year}",
                schema_mode="merge", 
                storage_options= STORAGE_OPTIONS,
            )
        else:
            write_deltalake(
                SILVER_URI,
                silver_table,
                mode = 'overwrite',
                partition_by=["source_year", "source_month"],
                schema_mode="overwrite",
                storage_options= STORAGE_OPTIONS,
            )

        print(f"   Silver write complete for {year} \n")
        return True
    except Exception as e:
        print(f"[ERROR] {e}")
        return False
    
    finally:
        con.close() # Close even on error



# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    #Sets up the parser for the CLI call of this file
    parser = argparse.ArgumentParser(description="Ingest NOAA GSOD weather archive files")
    parser.add_argument("--year-start", type=int, default=2023, help="First year in range")
    parser.add_argument("--year-end",   type=int, default=2023, help="Last year in range")

    args = parser.parse_args()

    # Build the year range
    if args.year_start > args.year_end:
            print(f"[ERROR]: Year start ({args.year_start}) is greater than year end ({args.year_end})")
            exit(1)

    years = range(args.year_start, args.year_end + 1)

    print("NOAA Weather Silver Transformation")
    print(f"Years: {", ".join(str(year) for year in years)}")

    results = {"success": [], "failed": []}

    for year in years:
        if transform_year(year):
            results["success"].append(f"{year}")
        else:
            results["failed"].append(f"{year}")

    # Final summary
    print(f"\n{'=' * 55}")
    print(f"SILVER TRANSFORMATION SUMMARY")
    print(f"{'=' * 55}")
    print(f"  Successful : {len(results['success'])} years")
    print(f"  Failed     : {len(results['failed'])} years")
    if results["failed"]:
        print(f"  Failed     : {', '.join(results['failed'])}")
    print(f"  Silver table: {SILVER_URI}")
    print(f"{'=' * 55}")

    if results["failed"]:
        exit(1)

if __name__ == "__main__":
    main()