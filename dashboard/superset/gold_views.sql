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

-- Retail Domain

-- Github Archive Domain

-- Weather Domain


