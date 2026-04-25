-- DuckDB setup for Superset: secrets and views over Gold Delta tables
-- Run this inside DuckDB (or via duckdb_setup.py) to create the
-- necessary S3 secret and analytical views.

CREATE OR REPLACE SECRET minio_secret (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'minio:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

-- ── Creating Views Over Gold Delta tables ─────────────────────────

-- Taxi Domain
CREATE OR REPLACE VIEW taxi_daily_summary AS
SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/daily_summary/**/*.parquet');

CREATE OR REPLACE VIEW taxi_individual_day_summary AS
SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/individual_day_summary/**/*.parquet');

CREATE OR REPLACE VIEW taxi_zone_performance AS
SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/zone_performance/**/*.parquet');

CREATE OR REPLACE VIEW taxi_hourly_summary AS
SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/hourly_summary/**/*.parquet');

CREATE OR REPLACE VIEW taxi_payment_summary AS
SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/payment_summary/**/*.parquet');

CREATE OR REPLACE VIEW taxi_borough_summary AS
SELECT * FROM read_parquet('s3://gold/taxi/yellow_tripdata/borough_summary/**/*.parquet');

-- GitHub Archive Domain
CREATE OR REPLACE VIEW github_event_summary_hour AS
SELECT * FROM read_parquet('s3://gold/github_archive/event_summary_hour/**/*.parquet');

CREATE OR REPLACE VIEW github_top_repos AS
SELECT * FROM read_parquet('s3://gold/github_archive/top_repos/**/*.parquet');

CREATE OR REPLACE VIEW github_daily_activity AS
SELECT * FROM read_parquet('s3://gold/github_archive/daily_activity/**/*.parquet');

-- Retail Domain (Instacart)
CREATE OR REPLACE VIEW retail_department_performance AS
SELECT * FROM read_parquet('s3://gold/retail/department_performance/**/*.parquet');

CREATE OR REPLACE VIEW retail_aisle_popularity AS
SELECT * FROM read_parquet('s3://gold/retail/aisle_popularity/**/*.parquet');

CREATE OR REPLACE VIEW retail_product_reorder_analysis AS
SELECT * FROM read_parquet('s3://gold/retail/product_reorder_analysis/**/*.parquet');

-- Weather Domain (NOAA)
CREATE OR REPLACE VIEW weather_monthly_climate AS
SELECT * FROM read_parquet('s3://gold/weather/monthly_climate/**/*.parquet');

CREATE OR REPLACE VIEW weather_station_metrics AS
SELECT * FROM read_parquet('s3://gold/weather/station_metrics/**/*.parquet');

CREATE OR REPLACE VIEW weather_daily_extremes AS
SELECT * FROM read_parquet('s3://gold/weather/daily_extremes/**/*.parquet');