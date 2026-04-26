-- Answers: What are the long-term climate statistics per station?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/weather/staging/station_metrics.parquet',
    format        = 'parquet',
    tags          = ['weather', 'gold']
) }}

SELECT
    station,
    station_name,
    latitude,
    longitude,
    elevation,
    source_year,
    source_month,
    COUNT(*)                      AS total_observations,
    MIN(date)                     AS first_observation,
    MAX(date)                     AS last_observation,
    COUNT(DISTINCT source_year)   AS years_of_data,

    -- Temperature metrics (Fahrenheit)
    ROUND(AVG(temp_f), 2)         AS avg_temp_f,
    ROUND(MIN(temp_f), 2)         AS record_low_temp_f,
    ROUND(MAX(temp_f), 2)         AS record_high_temp_f,
    ROUND(STDDEV(temp_f), 2)      AS temp_stddev_f,

    -- Temperature metrics (Celsius)
    ROUND(AVG(temp_c), 2)         AS avg_temp_c,
    ROUND(MIN(temp_c), 2)         AS record_low_temp_c,
    ROUND(MAX(temp_c), 2)         AS record_high_temp_c,

    -- Precipitation
    ROUND(AVG(prcp_in), 2)        AS avg_daily_precipitation_in,
    ROUND(SUM(prcp_in), 2)        AS total_precipitation_in,
    COUNT(*) FILTER (WHERE has_precipitation = true) AS precipitation_days,
    ROUND(
        COUNT(*) FILTER (WHERE has_precipitation = true) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                             AS precipitation_frequency_pct,

    -- Wind
    ROUND(AVG(wdsp_kts), 2)       AS avg_wind_speed_kts,
    ROUND(AVG(wdsp_kmh), 2)       AS avg_wind_speed_kmh,
    ROUND(MAX(gust_kts), 2)       AS max_gust_recorded_kts,

    -- Freezing
    COUNT(*) FILTER (WHERE is_freezing = true)  AS freezing_days,
    ROUND(
        COUNT(*) FILTER (WHERE is_freezing = true) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                             AS freezing_frequency_pct,

    -- Extreme heat
    COUNT(*) FILTER (WHERE is_extreme_heat = true) AS extreme_heat_days,
    ROUND(
        COUNT(*) FILTER (WHERE is_extreme_heat = true) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                             AS extreme_heat_frequency_pct,

    -- Snow
    COUNT(*) FILTER (WHERE sndp_in > 0) AS snow_days,
    ROUND(AVG(CASE WHEN sndp_in > 0 THEN sndp_in END), 2) AS avg_snow_depth_in,

    -- Visibility
    ROUND(AVG(visib_mi), 2)       AS avg_visibility_mi,

    -- Gale days
    COUNT(*) FILTER (WHERE is_gale = true) AS gale_days

FROM {{ ref('silver_weather_data') }}
GROUP BY
    station,
    station_name,
    latitude,
    longitude,
    elevation,
    source_year,
    source_month
ORDER BY
    total_observations DESC