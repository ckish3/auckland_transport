SELECT
    delay_category,
    COUNT(*) as delay_count
FROM {{ ref('latest_data') }}
GROUP BY delay_category