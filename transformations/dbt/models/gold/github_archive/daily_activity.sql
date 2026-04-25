-- Answers: How many events occur each day, broken down by type?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/github_archive/staging/daily_activity.parquet',
    format        = 'parquet',
    tags          = ['github', 'gold']
) }}

SELECT
    year,
    month,
    day,
    COUNT(*)                             AS total_events,
    COUNT(DISTINCT actor_login)          AS unique_actors,
    COUNT(DISTINCT repo_name)            AS unique_repos,
    COUNT(DISTINCT event_type)           AS event_types_count,
    COUNT(*) FILTER (WHERE public = true) AS public_events
FROM {{ ref('silver_github_data') }}
GROUP BY
    year,
    month,
    day
ORDER BY
    year,
    month,
    day