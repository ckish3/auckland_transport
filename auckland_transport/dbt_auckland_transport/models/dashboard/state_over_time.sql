-- This table gives the delay category breakdown by day over time

WITH day_total AS (
    SELECT
        response_date,
        COUNT(1) as day_number
    FROM {{ ref('latest_data') }}
    GROUP BY response_date
),

category_count AS (

    SELECT
        response_date,
        delay_category,
        COUNT(1) as delay_count
    FROM {{ ref('latest_data') }}
    GROUP BY response_date, delay_category
)

SELECT
    c.response_date,
    c.delay_category,
    c.delay_count / (1.0 * t.day_number) as proportion
FROM category_count c
LEFT JOIN day_total t
ON c.response_date = t.response_date