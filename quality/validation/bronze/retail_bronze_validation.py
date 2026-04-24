"""
Instacart Retail Bronze Validation
==================================
Validates Bronze Delta tables (order_products, products, aisles, departments).
"""

import json
import sys
from pathlib import Path
import great_expectations as gx
import pandas as pd
from deltalake import DeltaTable

REPORT_DIR = Path("quality/reports/bronze/retail")
STORAGE_OPTIONS = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name": "us-east-1",
    "allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

TABLES = {
    "order_products": {
        "uri": "s3://bronze/retail/order_products",
        "min_rows": 30_000_000,  # ~32M in full dataset
        "max_rows": 35_000_000,
        "columns": ["order_id", "product_id", "add_to_cart_order", "reordered",
                    "product_name", "aisle_name", "department_name",
                    "ingested_at", "source_file"],
        "non_null_cols": ["order_id", "product_id", "ingested_at"]
    },
    "products": {
        "uri": "s3://bronze/retail/products",
        "min_rows": 49_000,
        "max_rows": 51_000,
        "columns": ["product_id", "product_name", "aisle_id", "department_id",
                    "ingested_at", "source_file"],
        "non_null_cols": ["product_id", "product_name"]
    },
    "aisles": {
        "uri": "s3://bronze/retail/aisles",
        "min_rows": 130,
        "max_rows": 140,
        "columns": ["aisle_id", "aisle", "ingested_at", "source_file"],
        "non_null_cols": ["aisle_id", "aisle"]
    },
    "departments": {
        "uri": "s3://bronze/retail/departments",
        "min_rows": 20,
        "max_rows": 25,
        "columns": ["department_id", "department", "ingested_at", "source_file"],
        "non_null_cols": ["department_id", "department"]
    }
}

def load_table(table_name: str) -> pd.DataFrame:
    dt = DeltaTable(TABLES[table_name]["uri"], storage_options=STORAGE_OPTIONS)
    return dt.to_pyarrow_table().to_pandas()

def validate_table(context, table_name: str, df: pd.DataFrame):
    config = TABLES[table_name]
    suite_name = f"bronze_retail_{table_name}"
    try:
        context.suites.delete(suite_name)
    except:
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=config["min_rows"], max_value=config["max_rows"]
        )
    )
    for col in config["columns"]:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))
    for col in config["non_null_cols"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col, mostly=0.99)
        )

    data_source = context.data_sources.add_pandas(name=f"retail_{table_name}")
    data_asset = data_source.add_dataframe_asset(name="batch")
    batch_def = data_asset.add_batch_definition_whole_dataframe("whole")
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"val_{table_name}", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name=f"cp_{table_name}", validation_definitions=[val_def])
    )
    return checkpoint.run(batch_parameters={"dataframe": df})

def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    context = gx.get_context()
    all_passed = True
    for table_name in TABLES:
        print(f"\nValidating {table_name}...")
        try:
            df = load_table(table_name)
            print(f"  Loaded {len(df):,} rows")
        except Exception as e:
            print(f"  ❌ Failed to load: {e}")
            all_passed = False
            continue
        result = validate_table(context, table_name, df)
        passed = result.success
        print(f"  Status: {'✅ PASSED' if passed else '❌ FAILED'}")
        report_path = REPORT_DIR / f"{table_name}.json"
        with open(report_path, "w") as f:
            json.dump(result.describe_dict(), f, indent=2, default=str)
        if not passed:
            all_passed = False

    if not all_passed:
        print("\n❌ Bronze validation failed for retail.")
        sys.exit(1)
    print("\n✅ All retail bronze tables passed.")

if __name__ == "__main__":
    main()