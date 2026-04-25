-- Staging model for NOAA Weather Silver data
-- Reads the Silver Parquet and passes all columns through.
-- Gold models will reference this for aggregation.

SELECT
    STATION,
    DATE,
    LATITUDE,
    LONGITUDE,
    ELEVATION,
    NAME,
    TEMP,
    DEWP,
    SLP,
    STP,
    VISIB,
    WDSP,
    MXSPD,
    GUST,
    MAX,
    MIN,
    PRCP,
    SNDP,
    FRSHTT,
    year,
    month,
    day_of_week,
    season,
    is_freezing,
    has_precipitation
FROM read_parquet('s3://silver/weather/gsod_silver.parquet')
-- Static dataset – no filtering needed.