{{ config(
    materialized='table'
) }}

WITH source_comics AS (
    SELECT 
        comic_id,
        title,
        LENGTH(title) AS title_len,  -- Length of the title
        LENGTH(title) * 5 AS cost,  -- Cost calculation based on title length
        publish_date,
        DAYNAME(publish_date) AS publish_day,  -- Get the weekday name
        YEAR(publish_date) AS publish_year,
        MONTH(publish_date) AS publish_month,
        CASE 
            WHEN LENGTH(title) * 5 BETWEEN 5 AND 30 THEN '1 - Low'
            WHEN LENGTH(title) * 5 BETWEEN 35 AND 60 THEN '2 - Medium'
            WHEN LENGTH(title) * 5 BETWEEN 65 AND 95 THEN '3 - Medium - High'
            WHEN LENGTH(title) * 5 BETWEEN 100 AND 150 THEN '4 - High'
            ELSE '5 - Too High'
        END AS cost_label
    FROM {{ source('analytics', 'comics') }}
)

SELECT * FROM source_comics
