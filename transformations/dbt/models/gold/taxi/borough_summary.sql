-- Answers: How does each pickup borough perform overall?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/taxi/staging/borough_summary.parquet',
    format        = 'parquet',
    tags          = ['taxi', 'gold']
) }}

SELECT
    pickup_borough,
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

    -- Peak hour (median pickup hour for the borough)
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY pickup_hour) AS peak_pickup_hour

FROM {{ ref('taxi_data_with_zones') }}
GROUP BY
    pickup_borough,
    source_year,
    source_month
ORDER BY total_trips DESC