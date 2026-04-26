-- Answers: How did each individual date perform in terms of trips, revenue, and efficiency?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/taxi/staging/individual_day_summary.parquet',
    format        = 'parquet',
) }}


SELECT
    pickup_date,
    source_year,
    source_month,
    pickup_day_of_week,
    is_weekend,
    day_name, 

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

    -- Peak hour (most common pickup hour that day)
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY pickup_hour) AS peak_pickup_hour,

    -- Payment breakdown
    COUNT(*) FILTER (WHERE payment_type = 1) AS credit_card_trips,
    COUNT(*) FILTER (WHERE payment_type = 2) AS cash_trips,
    COUNT(*) FILTER (WHERE payment_type = 3) AS no_charge_trips

FROM {{ ref('taxi_data_with_zones') }}
GROUP BY
    pickup_date,
    source_year,
    source_month,
    pickup_day_of_week,
    day_name,
    is_weekend
ORDER BY pickup_date
