-- Answers: On which dates did each station experience its most extreme weather?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/weather/staging/daily_extremes.parquet',
    format        = 'parquet',
    tags          = ['weather', 'gold']
) }}

SELECT
    STATION,
    NAME,
    LATITUDE,
    LONGITUDE,
    DATE,
    year,
    month,
    season,
    
    -- Temperature extremes
    TEMP,
    MAX AS max_temp_that_day,
    MIN AS min_temp_that_day,
    is_freezing,
    
    -- Precipitation
    PRCP,
    has_precipitation,
    SNDP AS snow_depth,
    
    -- Wind
    WDSP AS wind_speed,
    MXSPD AS max_sustained_wind,
    GUST AS wind_gust,
    
    -- Flags
    CASE WHEN TEMP = (SELECT MAX(TEMP) FROM {{ ref('silver_weather_data') }} s2 WHERE s2.STATION = s1.STATION)
         THEN TRUE ELSE FALSE END AS is_record_high,
    CASE WHEN TEMP = (SELECT MIN(TEMP) FROM {{ ref('silver_weather_data') }} s2 WHERE s2.STATION = s1.STATION)
         THEN TRUE ELSE FALSE END AS is_record_low,
    CASE WHEN PRCP = (SELECT MAX(PRCP) FROM {{ ref('silver_weather_data') }} s2 WHERE s2.STATION = s1.STATION)
         THEN TRUE ELSE FALSE END AS is_record_precipitation,
    CASE WHEN GUST = (SELECT MAX(GUST) FROM {{ ref('silver_weather_data') }} s2 WHERE s2.STATION = s1.STATION)
         THEN TRUE ELSE FALSE END AS is_record_gust

FROM {{ ref('silver_weather_data') }} s1
WHERE
    -- Only include days that are extremes for the station
    TEMP = (SELECT MAX(TEMP) FROM {{ ref('silver_weather_data') }} s2 WHERE s2.STATION = s1.STATION)
    OR TEMP = (SELECT MIN(TEMP) FROM {{ ref('silver_weather_data') }} s2 WHERE s2.STATION = s1.STATION)
    OR PRCP = (SELECT MAX(PRCP) FROM {{ ref('silver_weather_data') }} s2 WHERE s2.STATION = s1.STATION)
    OR GUST = (SELECT MAX(GUST) FROM {{ ref('silver_weather_data') }} s2 WHERE s2.STATION = s1.STATION)
ORDER BY
    STATION,
    DATE