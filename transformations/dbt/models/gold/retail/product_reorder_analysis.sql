-- Answers: Which products are most frequently reordered, and what is their typical cart position?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/retail/staging/product_reorder_analysis.parquet',
    format        = 'parquet',
    tags          = ['retail', 'gold']
) }}

SELECT
    product_id,
    product_name,
    aisle_name,
    department_name,
    COUNT(*)                            AS total_order_items,
    COUNT(DISTINCT order_id)            AS total_orders,
    SUM(reordered_flag)                 AS total_reorders,
    ROUND(AVG(reordered_flag), 4)       AS reorder_rate,
    ROUND(AVG(add_to_cart_order), 2)    AS avg_add_to_cart_position,
    SUM(is_first_item)                  AS first_item_count,
    ROUND(AVG(is_first_item), 4)        AS first_item_rate
FROM {{ ref('silver_retail_data') }}
GROUP BY
    product_id,
    product_name,
    aisle_name,
    department_name
ORDER BY
    reorder_rate DESC