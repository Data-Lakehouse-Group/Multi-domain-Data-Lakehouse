-- Answers: How does each department perform in terms of orders, reorders, and product variety?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/retail/staging/department_performance.parquet',
    format        = 'parquet',
    tags          = ['retail', 'gold']
) }}

SELECT
    department_name,
    COUNT(*)                            AS total_order_items,
    COUNT(DISTINCT order_id)            AS total_orders,
    COUNT(DISTINCT product_id)          AS unique_products,
    SUM(reordered_flag)                 AS total_reorders,
    ROUND(AVG(reordered_flag), 4)       AS avg_reorder_rate,
    ROUND(AVG(add_to_cart_order), 2)    AS avg_add_to_cart_position,
    SUM(is_first_item)                  AS first_item_count,
    ROUND(AVG(is_first_item), 4)        AS first_item_rate
FROM {{ ref('silver_retail_data') }}
GROUP BY
    department_name
ORDER BY
    total_order_items DESC