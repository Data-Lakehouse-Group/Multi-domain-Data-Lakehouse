-- Answers: How do payment methods compare in terms of revenue and tipping behavior?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/taxi/staging/payment_summary.parquet',
    format        = 'parquet',
    tags          = ['taxi', 'gold']
) }}

SELECT
    payment_type,
    payment_method,
    source_year,
    source_month,

    -- Volume metrics
    COUNT(*)                    AS total_trips,
    SUM(passenger_count)        AS total_passengers,

    -- Revenue metrics
    ROUND(SUM(fare_amount), 2)      AS total_fare_revenue,
    ROUND(SUM(tip_amount), 2)       AS total_tips,
    ROUND(SUM(tolls_amount), 2)     AS total_tolls,
    ROUND(SUM(total_amount), 2)     AS total_revenue,

    -- Average metrics
    ROUND(AVG(fare_amount), 2)              AS avg_fare,
    ROUND(AVG(tip_amount), 2)               AS avg_tip,
    ROUND(AVG(trip_distance), 2)            AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2)    AS avg_trip_duration_minutes,
    ROUND(AVG(passenger_count), 2)          AS avg_passengers_per_trip,

    -- Efficiency metrics
    ROUND(
        SUM(fare_amount) / NULLIF(SUM(trip_distance), 0), 2
    ) AS revenue_per_mile,
    ROUND(
        SUM(fare_amount) / NULLIF(SUM(trip_duration_minutes), 0), 2
    ) AS revenue_per_minute,
    ROUND(
        SUM(tip_amount) / NULLIF(SUM(fare_amount), 0) * 100, 2
    ) AS tip_percentage,

    -- Peak hour (most common pickup hour for each payment type)
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY pickup_hour) AS peak_pickup_hour

FROM {{ ref('taxi_data_with_zones') }}
GROUP BY
    payment_type,
    source_year,
    source_month,
    payment_method,
ORDER BY payment_type