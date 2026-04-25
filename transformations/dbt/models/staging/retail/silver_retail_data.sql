-- Staging model for Instacart Retail Silver data
-- Simply reads the Silver Parquet file and passes through all columns.
-- The Gold models will aggregate from this source.

SELECT
    order_id,
    product_id,
    add_to_cart_order,
    reordered,
    reordered_flag,
    is_first_item,
    product_name,
    aisle_name,
    department_name,
    bronze_ingested_at,
    source_file,
    silver_processed_at
FROM read_parquet('s3://silver/retail/retail_silver.parquet')
-- Static dataset – no filtering needed.