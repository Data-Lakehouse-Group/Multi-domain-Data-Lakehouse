-- Answers: What are the long-term climate statistics per station?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/weather/staging/station_metrics.parquet',
    format        = 'parquet',
    tags          = ['weather', 'gold']
) }}

SELECT
    STATION,
    NAME,
    LATITUDE,
    LONGITUDE,
    ELEVATION,
    COUNT(*)                      AS total_observations,
    MIN(DATE)                     AS first_observation,
    MAX(DATE)                     AS last_observation,
    COUNT(DISTINCT year)          AS years_of_data,
    
    -- Temperature metrics
    ROUND(AVG(TEMP), 2)           AS avg_temp,
    ROUND(MIN(TEMP), 2)           AS record_low_temp,
    ROUND(MAX(TEMP), 2)           AS record_high_temp,
    ROUND(STDDEV(TEMP), 2)        AS temp_stddev,
    
    -- Precipitation
    ROUND(AVG(PRCP), 2)           AS avg_daily_precipitation,
    ROUND(SUM(PRCP), 2)           AS total_precipitation,
    COUNT(*) FILTER (WHERE has_precipitation = 1) AS precipitation_days,
    ROUND(
        COUNT(*) FILTER (WHERE has_precipitation = 1) * 100.0 / NULLIF(COUNT(*), 0), 2
    ) AS precipitation_frequency,
    
    -- Wind
    ROUND(AVG(WDSP), 2)           AS avg_wind_speed,
    ROUND(MAX(GUST), 2)           AS max_gust_recorded,
    
    -- Freezing
    COUNT(*) FILTER (WHERE is_freezing = 1) AS freezing_days,
    ROUND(
        COUNT(*) FILTER (WHERE is_freezing = 1) * 100.0 / NULLIF(COUNT(*), 0), 2
    ) AS freezing_frequency,
    
    -- Snow (SNDP > 0 indicates snow depth was recorded)
    COUNT(*) FILTER (WHERE SNDP > 0) AS snow_days,
    
    -- Average visibility
    ROUND(AVG(VISIB), 2)          AS avg_visibility

FROM {{ ref('silver_weather_data') }}
GROUP BY
    STATION,
    NAME,
    LATITUDE,
    LONGITUDE,
    ELEVATION
ORDER BY
    total_observations DESC