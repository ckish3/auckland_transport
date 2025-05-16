--Tests that day_type values are roughly in the right portion

with day_count as (
    SELECT
        COUNT( day_type) as total_rows
    FROM {{ ref('time_columns') }}
),

day_type_count as (
    SELECT
        day_type,
        COUNT( day_type) as day_type_rows
    FROM {{ ref('time_columns') }}
    GROUP BY day_type
)

SELECT
    a.*
FROM day_type_count a, day_count b
WHERE a.day_type_rows / (1.0 * b.total_rows) < 0.1
    OR a.day_type_rows / (1.0 * b.total_rows) > 0.9