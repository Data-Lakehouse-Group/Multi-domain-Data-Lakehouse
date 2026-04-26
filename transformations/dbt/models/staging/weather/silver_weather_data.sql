SELECT
    -- Original columns with optimised storage types
    STATION     AS station,
    DATE        AS date,
    LATITUDE    AS latitude,
    LONGITUDE   AS longitude,
    ELEVATION   AS elevation,
    NAME        AS station_name,

    -- Core weather metrics
    TEMP        AS temp_f,
    DEWP        AS dewp_f,
    SLP         AS slp_hpa,
    STP         AS stp_hpa,
    VISIB       AS visib_mi,
    WDSP        AS wdsp_kts,
    MXSPD       AS mxspd_kts,
    GUST        AS gust_kts,
    MAX         AS max_temp_f,
    MIN         AS min_temp_f,
    PRCP        AS prcp_in,
    SNDP        AS sndp_in,

    -- Bronze audit columns
    source_year,
    source_month,
    source_file,
    ingested_at,

    -- Derived: date dimensions
    season_northern,
    day_name,
    is_weekend,

    -- Derived: unit conversions (Celsius)
    temp_c,
    max_temp_c,
    min_temp_c,

    -- Derived: unit conversions (km/h)
    wdsp_kmh,
    mxspd_kmh,
    gust_kmh,

    -- Derived: weather event flags
    is_freezing,
    is_extreme_heat,
    has_precipitation,
    is_gale,

    -- Silver audit column
    processed_at

FROM read_parquet(
    's3://silver/weather/**/*.parquet',
    hive_partitioning = true
)
WHERE source_year  = {{ var('year',  2023) }}