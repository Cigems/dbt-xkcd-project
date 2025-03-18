{{ config(
    materialized='table'
) }}

WITH source_stats AS (
    SELECT 
        cs.comic_id,
        cs.record_date,
        case when DAYNAME(cs.record_date) in ('Mon','Fri','Wed') then 1 else 0 end AS publish_day_flag,
        cs.views,
        cs.reviews,
        DATEDIFF(DAY, dc.publish_date, cs.record_date) +1 AS days_since_published,
        SUM(cs.views) OVER (PARTITION BY cs.comic_id ORDER BY cs.record_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS views_since_published,
        AVG(cs.reviews) OVER (PARTITION BY cs.comic_id ORDER BY cs.record_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_reviews_since_published
    FROM {{ source('analytics', 'comics_stats') }} cs
    JOIN {{ ref('dim_comics') }} dc 
        ON cs.comic_id = dc.comic_id
)

SELECT * FROM source_stats
