-- Answers: What are the monthly climate statistics per station?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/weather/staging/monthly_climate.parquet',
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
    season_northern,

    -- Observation count
    COUNT(*)                                                AS observation_days,

    -- Temperature (Fahrenheit)
    ROUND(AVG(temp_f), 2)                                   AS avg_temp_f,
    ROUND(MIN(temp_f), 2)                                   AS min_temp_f,
    ROUND(MAX(temp_f), 2)                                   AS max_temp_f,
    ROUND(STDDEV(temp_f), 2)                                AS temp_stddev_f,

    -- Temperature (Celsius)
    ROUND(AVG(temp_c), 2)                                   AS avg_temp_c,
    ROUND(MIN(temp_c), 2)                                   AS min_temp_c,
    ROUND(MAX(temp_c), 2)                                   AS max_temp_c,

    -- Precipitation
    ROUND(SUM(prcp_in), 2)                                  AS total_precipitation_in,
    ROUND(AVG(prcp_in), 2)                                  AS avg_daily_precipitation_in,
    COUNT(*) FILTER (WHERE has_precipitation = true)         AS days_with_precipitation,
    ROUND(
        COUNT(*) FILTER (WHERE has_precipitation = true) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                       AS precipitation_frequency_pct,

    -- Wind
    ROUND(AVG(wdsp_kts), 2)                                 AS avg_wind_speed_kts,
    ROUND(AVG(wdsp_kmh), 2)                                 AS avg_wind_speed_kmh,
    ROUND(MAX(mxspd_kts), 2)                                AS max_wind_speed_kts,
    ROUND(MAX(gust_kts), 2)                                 AS max_gust_kts,
    COUNT(*) FILTER (WHERE is_gale = true)                   AS gale_days,

    -- Temperature events
    COUNT(*) FILTER (WHERE is_freezing = true)               AS freezing_days,
    ROUND(
        COUNT(*) FILTER (WHERE is_freezing = true) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                       AS freezing_frequency_pct,
    COUNT(*) FILTER (WHERE is_extreme_heat = true)           AS extreme_heat_days,
    ROUND(
        COUNT(*) FILTER (WHERE is_extreme_heat = true) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                       AS extreme_heat_frequency_pct,

    -- Snow
    COUNT(*) FILTER (WHERE sndp_in > 0)                     AS snow_days,
    ROUND(AVG(CASE WHEN sndp_in > 0 THEN sndp_in END), 2)  AS avg_snow_depth_in,
    ROUND(MAX(sndp_in), 2)                                  AS max_snow_depth_in,

    -- Visibility
    ROUND(AVG(visib_mi), 2)                                 AS avg_visibility_mi,
    ROUND(MIN(visib_mi), 2)                                 AS min_visibility_mi

FROM {{ ref('silver_weather_data') }}
GROUP BY
    station,
    station_name,
    latitude,
    longitude,
    elevation,
    source_year,
    source_month,
    season_northern
ORDER BY
    station,
    source_year,
    source_month