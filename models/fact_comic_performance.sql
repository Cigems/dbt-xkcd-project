{{ config(
    materialized='table'
) }}

WITH aggregated_stats AS (
    SELECT 
        comic_id,
        CASE WHEN SUM(views) > 0 THEN MIN(CASE WHEN views > 0 THEN days_since_published ELSE 10000 END) ELSE null END AS first_seen_day,
        CASE WHEN SUM(views) > 0 THEN MAX(CASE WHEN views > 0 THEN days_since_published ELSE 1 END) ELSE null END AS last_seen_day,
        MAX(days_since_published) AS days_since_published,
        SUM(CASE WHEN days_since_published <= 7 THEN views ELSE 0 END) AS views_7d,
        SUM(CASE WHEN days_since_published <= 30 THEN views ELSE 0 END) AS views_30d,
        SUM(CASE WHEN days_since_published <= 60 THEN views ELSE 0 END) AS views_60d,
        SUM(CASE WHEN days_since_published <= 90 THEN views ELSE 0 END) AS views_90d,
        AVG(CASE WHEN days_since_published <= 7 THEN reviews ELSE NULL END) AS avg_reviews_7d,
        AVG(CASE WHEN days_since_published <= 30 THEN reviews ELSE NULL END) AS avg_reviews_30d,
        AVG(CASE WHEN days_since_published <= 60 THEN reviews ELSE NULL END) AS avg_reviews_60d,
        AVG(CASE WHEN days_since_published <= 90 THEN reviews ELSE NULL END) AS avg_reviews_90d
    FROM {{ ref('fact_comic_stats') }}
    GROUP BY comic_id
)

SELECT * FROM aggregated_stats
