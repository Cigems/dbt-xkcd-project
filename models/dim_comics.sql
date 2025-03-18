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
        MONTH(publish_date) AS publish_month
    FROM {{ source('analytics', 'comics') }}
)

SELECT * FROM source_comics
