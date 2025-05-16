--Test if both day types are present

WITH day_count as (

    SELECT
        COUNT(DISTINCT day_type) as num_days
    FROM {{ ref('time_columns') }}
)

SELECT
    *
FROM day_count
WHERE num_days != 2