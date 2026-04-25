-- Answers: How do events break down by hour and type?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/github_archive/staging/event_summary_hour.parquet',
    format        = 'parquet',
    tags          = ['github', 'gold']
) }}

SELECT
    year,
    month,
    day,
    hour,
    event_type,
    COUNT(*)            AS total_events,
    COUNT(DISTINCT actor_login) AS unique_actors,
    COUNT(DISTINCT repo_name)   AS unique_repos,
    COUNT(*) FILTER (WHERE public = true) AS public_events
FROM {{ ref('silver_github_data') }}
GROUP BY
    year,
    month,
    day,
    hour,
    event_type
ORDER BY
    year,
    month,
    day,
    hour