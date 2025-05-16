--Test that time__columns has rows for every day

WITH day_count as (

    SELECT
        COUNT(DISTINCT day_of_week) as num_days
    FROM {{ ref('time_columns') }}
)

SELECT
    *
FROM day_count
WHERE num_days != 7