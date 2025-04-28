-- This table gives the delay category breakdown by day over time

WITH day_total AS (
    SELECT
        day_of_week,
        COUNT(1) as day_number
    FROM {{ ref('latest_data') }}
    GROUP BY day_of_week
),

category_count AS (

    SELECT
        day_of_week,
        delay_category,
        COUNT(1) as delay_count
    FROM {{ ref('latest_data') }}
    GROUP BY day_of_week, delay_category
)

SELECT
    c.day_of_week,
    c.delay_category,
    c.delay_count / (1.0 * t.day_number) as proportion
FROM category_count c
LEFT JOIN day_total t
ON c.day_of_week = t.day_of_week