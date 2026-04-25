-- Answers: What are the monthly climate statistics per station?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/weather/staging/monthly_climate.parquet',
    format        = 'parquet',
    tags          = ['weather', 'gold']
) }}

SELECT
    STATION,
    NAME,
    LATITUDE,
    LONGITUDE,
    ELEVATION,
    year,
    month,

    -- Temperature
    ROUND(AVG(TEMP), 2)         AS avg_temp,
    ROUND(MIN(TEMP), 2)         AS min_temp,
    ROUND(MAX(TEMP), 2)         AS max_temp,
    ROUND(AVG(MAX), 2)          AS avg_max_temp,
    ROUND(AVG(MIN), 2)          AS avg_min_temp,

    -- Precipitation
    ROUND(SUM(PRCP), 2)         AS total_precipitation,
    ROUND(AVG(PRCP), 2)         AS avg_daily_precipitation,
    COUNT(*) FILTER (WHERE has_precipitation = 1) AS days_with_precipitation,

    -- Wind
    ROUND(AVG(WDSP), 2)         AS avg_wind_speed,
    ROUND(MAX(MXSPD), 2)        AS max_wind_speed,
    ROUND(MAX(GUST), 2)         AS max_gust,

    -- Freezing days
    COUNT(*) FILTER (WHERE is_freezing = 1) AS freezing_days,

    -- Visibility
    ROUND(AVG(VISIB), 2)        AS avg_visibility,

    -- Record count
    COUNT(*)                    AS observation_days

FROM {{ ref('silver_weather_data') }}
GROUP BY
    STATION,
    NAME,
    LATITUDE,
    LONGITUDE,
    ELEVATION,
    year,
    month
ORDER BY
    STATION,
    year,
    month