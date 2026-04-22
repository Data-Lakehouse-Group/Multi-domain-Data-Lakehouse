-- ── Step 1: Install & load extensions ────────────────────────────────────────
INSTALL delta;
LOAD delta;
INSTALL httpfs;
LOAD httpfs;


-- ── Step 2: Configure MinIO connection ───────────────────────────────────────
SET s3_endpoint       = 'minio:9000';
SET s3_access_key_id  = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_use_ssl        = false;
SET s3_url_style      = 'path';
SET s3_region = 'us-east-1';
SET enable_http_metadata_cache = false;
SET s3_session_token = '';


-- ── Step 3: Creating Views Over Gold Delta tables ─────────────────────────

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

-- ── Step 4: Verify ───────────────────────────────────────────────────────────
-- Run these to confirm everything is wired up correctly

-- Check schema of a gold view
DESCRIBE taxi_daily_summary;
