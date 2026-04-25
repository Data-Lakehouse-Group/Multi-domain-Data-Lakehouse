-- Staging model for GitHub Archive Silver data
-- Simply reads the Silver Parquet file and passes through all columns.
-- The Gold models will aggregate from this source.

SELECT
    id,
    event_type,
    public,
    created_at,
    actor_login,
    actor_id,
    repo_name,
    repo_id,
    org_login,
    payload_raw,
    year,
    month,
    day,
    hour,
    day_of_week,
    event_time,
    source_file
FROM read_parquet('s3://silver/github/github_silver.parquet')
-- No filtering; the dataset is static and we process all available hours.