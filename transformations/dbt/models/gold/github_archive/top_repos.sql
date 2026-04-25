-- Answers: Which repositories have the most activity?

{{ config(
    materialized  = 'external',
    location      = 's3://artifacts/dbt/github_archive/staging/top_repos.parquet',
    format        = 'parquet',
    tags          = ['github', 'gold']
) }}

SELECT
    repo_name,
    year,
    month,
    COUNT(*)                    AS total_events,
    COUNT(DISTINCT event_type) AS event_types_count,
    COUNT(DISTINCT actor_login) AS unique_actors,
    COUNT(*) FILTER (WHERE event_type = 'PushEvent')          AS push_events,
    COUNT(*) FILTER (WHERE event_type = 'PullRequestEvent')   AS pull_request_events,
    COUNT(*) FILTER (WHERE event_type = 'IssuesEvent')        AS issues_events,
    COUNT(*) FILTER (WHERE event_type = 'WatchEvent')         AS watch_events,
    COUNT(*) FILTER (WHERE event_type = 'ForkEvent')          AS fork_events,
    COUNT(*) FILTER (WHERE event_type = 'CreateEvent')        AS create_events,
    COUNT(*) FILTER (WHERE event_type = 'DeleteEvent')        AS delete_events,
    COUNT(*) FILTER (WHERE event_type = 'PullRequestReviewEvent') AS pr_review_events
FROM {{ ref('silver_github_data') }}
GROUP BY
    repo_name,
    year,
    month
ORDER BY
    total_events DESC