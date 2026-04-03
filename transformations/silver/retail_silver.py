"""Silver Layer: Retail - reads bronze Delta tables and joins"""
import os, logging, io
import polars as pl
import boto3
from botocore.client import Config
from deltalake import DeltaTable

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

storage_opts = {
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ALLOW_HTTP": "true",
}

def read_delta(uri):
    dt = DeltaTable(uri, storage_options=storage_opts)
    return pl.from_arrow(dt.to_pyarrow_table())

def read_csv_from_minio(key):
    client = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY,
                          config=Config(signature_version="s3v4"))
    resp = client.get_object(Bucket=BRONZE_BUCKET, Key=key)
    return pl.read_csv(io.BytesIO(resp['Body'].read()))

def run():
    # Bronze tables: order_products, products, aisles, departments
    order_products = read_delta("s3://bronze/retail/order_products")
    products = read_csv_from_minio("retail/products.csv")  # stored as CSV in bronze
    aisles = read_csv_from_minio("retail/aisles.csv")
    departments = read_csv_from_minio("retail/departments.csv")

    # Convert types
    order_products = order_products.with_columns(pl.col("product_id").cast(pl.Int32))
    products = products.with_columns(pl.col("product_id").cast(pl.Int32),
                                     pl.col("aisle_id").cast(pl.Int32),
                                     pl.col("department_id").cast(pl.Int32))
    aisles = aisles.with_columns(pl.col("aisle_id").cast(pl.Int32))
    departments = departments.with_columns(pl.col("department_id").cast(pl.Int32))

    # Join
    df = order_products.join(products, on="product_id", how="left")
    df = df.join(aisles, on="aisle_id", how="left")
    df = df.join(departments, on="department_id", how="left")

    # Add audit columns
    import datetime
    df = df.with_columns([
        pl.lit(datetime.datetime.utcnow()).alias("ingested_at"),
        pl.lit("retail_silver").alias("source")
    ])

    # Write to silver
    client = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY,
                          config=Config(signature_version="s3v4"))
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client.put_object(Bucket=SILVER_BUCKET, Key="retail/retail_silver.parquet", Body=buf.getvalue())
    logging.info("Silver retail written")

if __name__ == "__main__":
    run()