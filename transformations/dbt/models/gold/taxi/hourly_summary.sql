-- Answers: How do trip metrics vary by hour of day?
-- This model aggregates across all days to show intra-day patterns.

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/taxi/staging/hourly_summary.parquet',
    format        = 'parquet',
    tags          = ['taxi', 'gold']
) }}

SELECT
    pickup_hour,
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

    -- Payment breakdown
    COUNT(*) FILTER (WHERE payment_type = 1) AS credit_card_trips,
    COUNT(*) FILTER (WHERE payment_type = 2) AS cash_trips,
    COUNT(*) FILTER (WHERE payment_type = 3) AS no_charge_trips

FROM {{ ref('taxi_data_with_zones') }}
GROUP BY
    pickup_hour,
    source_year,
    source_month
ORDER BY pickup_hour