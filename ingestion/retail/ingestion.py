"""
Instacart Bronze Ingestion — order_products__prior + lookup tables
==================================================================
Reads order_products__prior and its lookup tables (products, aisles,
departments), joins them into a single enriched table, and writes it
to a Delta table in MinIO partitioned by department_name.

Lookup tables are also written as-is to their own Delta tables.

Input:  data/raw/retail/{table}.csv
Output:
  s3://bronze/retail/order_products/  (partitioned)
  s3://bronze/retail/products/
  s3://bronze/retail/aisles/
  s3://bronze/retail/departments/

Usage:
    python ingestion/retail/ingestion.py
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.csv as pa_csv
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

INPUT_DIR = Path("data/raw/retail")

LOOKUP_TABLES = {
    "products": {
        "uri": "s3://bronze/retail/products",
    },
    "aisles": {
        "uri": "s3://bronze/retail/aisles",
    },
    "departments": {
        "uri": "s3://bronze/retail/departments",
    },
}

ORDER_PRODUCTS_URI = "s3://bronze/retail/order_products"

STORAGE_OPTIONS = {
    "endpoint_url"              : "http://localhost:9000",
    "aws_access_key_id"         : "minioadmin",
    "aws_secret_access_key"     : "minioadmin",
    "region_name"               : "us-east-1",
    "allow_http"                : "true",
    "aws_s3_allow_unsafe_rename": "true",
}

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS 
# ---------------------------------------------------------------------------

def read_csv(table_name: str) -> pa.Table:
    path = INPUT_DIR / f"{table_name}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Source file not found: {path}. Run ingestion/retail/download.py first."
        )
    return pa_csv.read_csv(path)


def add_audit_columns(table: pa.Table, source_file: str) -> pa.Table:
    now      = datetime.now(timezone.utc)
    num_rows = table.num_rows
    return (
        table
        .append_column("ingested_at", pa.array([now] * num_rows, type=pa.timestamp("us", tz="UTC")))
        .append_column("source_file", pa.array([source_file] * num_rows, type=pa.string()))
    )


def table_exists(uri: str) -> bool:
    try:
        DeltaTable(uri, storage_options=STORAGE_OPTIONS)
        return True
    except TableNotFoundError:
        return False

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    lookup_data = {}

    #Adding Lookup Tables
    for table_name, config in LOOKUP_TABLES.items():
        print(f"\nIngesting lookup table: {table_name}...")
        try:
            data = read_csv(table_name)
            lookup_data[table_name] = data #Add to lookup tables for join later
            data = add_audit_columns(data, f"{table_name}.csv")
            write_deltalake(
                config["uri"],
                data,
                mode            = "overwrite",
                schema_mode     = "overwrite",
                storage_options = STORAGE_OPTIONS,
            ) 
            print(f"  Written {data.num_rows:,} rows → {config['uri']}")
        except Exception as e:
            print(f"  ERROR ingesting {table_name}: {e}")
            return  # lookups are required — abort if any fail

    #Adding Main Order Products Table
    print("\nReading order_products__prior...")
    try:
        order_products_data = read_csv("order_products__prior")
        row_count = order_products_data.num_rows
        print(f"  Rows read: {row_count:,}")
    except FileNotFoundError as e:
        print(f"  ERROR: {e}")
        return
    
    print("  Adding audit columns...")
    order_products_data = add_audit_columns(order_products_data, "order_products__prior.csv")

    #Attaching lookup tables
    con = duckdb.connect()
    con.register("op", order_products_data)
    con.register("p", lookup_data["products"])
    con.register("a", lookup_data["aisles"])
    con.register("d", lookup_data["departments"])

    print("Attaching product, aisle and department tables...")
    order_products_data = con.execute("""
        SELECT
            op.*,
            p.product_name,
            a.aisle       AS aisle_name,
            d.department  AS department_name
        FROM op
        JOIN p ON op.product_id = p.product_id
        JOIN a ON p.aisle_id    = a.aisle_id
        JOIN d ON p.department_id = d.department_id
    """).arrow()

    print(f"Writing to Delta Lake at {ORDER_PRODUCTS_URI}...")
    try:
        if table_exists(ORDER_PRODUCTS_URI):
            #This is a static dataset so the predicate is not needed for rewriting and partitioning
            write_deltalake(
                ORDER_PRODUCTS_URI,
                order_products_data,
                mode            = "overwrite",
                schema_mode     = "merge",
                storage_options = STORAGE_OPTIONS,
            )
        else:
            write_deltalake(
                ORDER_PRODUCTS_URI,
                order_products_data,
                mode            = "overwrite",
                partition_by    = ["department_name"],
                schema_mode     = "overwrite",
                storage_options = STORAGE_OPTIONS,
            )
        print(f"Successfully ingested {row_count:,} row for Instacart Retail Dataset  ")
    except Exception as e:
        print(f"  ERROR writing order_products table: {e}")


if __name__ == "__main__":
    main()