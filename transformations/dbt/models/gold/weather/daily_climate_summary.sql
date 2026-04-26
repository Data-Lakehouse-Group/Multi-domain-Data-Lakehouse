-- Answers: What are the daily climate statistics per station?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/weather/staging/daily_climate.parquet',
    format        = 'parquet',
    tags          = ['weather', 'gold']
) }}

SELECT
    station,
    station_name,
    latitude,
    longitude,
    elevation,
    date,
    source_year,                 
    source_month,              
    day_name,
    is_weekend,
    season_northern,

    -- Temperature
    temp_f,
    temp_c,
    max_temp_f,
    max_temp_c,
    min_temp_f,
    min_temp_c,

    -- Precipitation
    prcp_in,
    has_precipitation,
    sndp_in,

    -- Wind
    wdsp_kts,
    wdsp_kmh,
    mxspd_kts,
    mxspd_kmh,
    gust_kts,
    gust_kmh,

    -- Temperature event flags
    is_freezing,
    is_extreme_heat,
    is_gale,

    -- Visibility
    visib_mi,

    -- Audit
    processed_at

FROM {{ ref('silver_weather_data') }}
ORDER BY
    station,
    date