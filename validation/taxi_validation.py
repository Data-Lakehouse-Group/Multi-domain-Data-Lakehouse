"""Great Expectations validation for taxi bronze data"""
import os
import pandas as pd
import great_expectations as ge
from deltalake import DeltaTable
from dotenv import load_dotenv

load_dotenv()
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

storage_opts = {
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ALLOW_HTTP": "true",
}

def run():
    dt = DeltaTable("s3://bronze/taxi/yellow_tripdata", storage_options=storage_opts)
    pdf = dt.to_pyarrow_table().to_pandas()
    dataset = ge.from_pandas(pdf)

    expectations = [
        dataset.expect_column_to_exist("tpep_pickup_datetime"),
        dataset.expect_column_values_to_not_be_null("fare_amount"),
        dataset.expect_column_values_to_be_between("fare_amount", 0, 500),
        dataset.expect_column_values_to_be_between("trip_distance", 0, 100),
        dataset.expect_column_values_to_be_of_type("passenger_count", "int64"),
    ]

    results = [e.success for e in expectations]
    if all(results):
        print("✅ Taxi validation PASSED")
    else:
        print("❌ Taxi validation FAILED")
        for e in expectations:
            if not e.success:
                print(f"  - Failed: {e.expectation_config['expectation_type']}")
    return all(results)

if __name__ == "__main__":
    run()