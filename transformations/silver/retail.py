"""
Instacart Retail Silver Transformation
======================================
Reads from the Bronze Delta table (order_products with joined
lookup fields), applies cleaning rules, derives new columns, and
writes a single Silver Parquet file to MinIO.

Because the Instacart dataset is static, we overwrite the entire
Silver file on each run.

Input:  s3://bronze/retail/order_products/
Output: s3://silver/retail/retail_silver.parquet

Usage:
    python transformations/silver/retail.py
"""

import os
import sys
import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BRONZE_URI = "s3://bronze/retail/order_products"
SILVER_URI  = "s3://silver/retail/retail_silver.parquet"

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
    """Read all Bronze order_products data (all partitions)."""
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
    - Deduplicate (if any)
    - Cast types
    - Add flags (e.g., is_reordered)
    - Add processed timestamp
    """
    con = duckdb.connect()
    con.register("bronze_retail", bronze_table)

    result = con.execute("""
        WITH cleaned AS (
            SELECT DISTINCT
                order_id,
                product_id,
                add_to_cart_order,
                reordered,
                product_name,
                aisle_name,
                department_name,
                ingested_at,
                source_file
            FROM bronze_retail
            WHERE
                order_id IS NOT NULL
                AND product_id IS NOT NULL
        )
        SELECT
            order_id,
            product_id,
            CAST(add_to_cart_order AS INTEGER) AS add_to_cart_order,
            CAST(reordered AS BOOLEAN) AS reordered,
            CAST(reordered AS INTEGER) AS reordered_flag, -- 0/1 for easier aggregation
            product_name,
            aisle_name,
            department_name,

            -- Derived: is_first_item (first added to cart)
            CASE WHEN add_to_cart_order = 1 THEN 1 ELSE 0 END AS is_first_item,

            -- Audit
            ingested_at AS bronze_ingested_at,
            source_file,
            CURRENT_TIMESTAMP AS silver_processed_at
        FROM cleaned
    """).fetch_arrow_table()

    return result

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    print("Instacart Retail Silver Transformation")
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