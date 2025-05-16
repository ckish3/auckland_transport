--Tests that the trip_update table has just 1 entry per trip and stop

with count_rows as (
    SELECT
        route_id,
        direction_id,
        start_time,
        start_date,
        stop_id,
        COUNT(*) as num_rows
    FROM {{ ref('trip_update') }}
    GROUP BY
        route_id,
        direction_id,
        start_time,
        start_date,
        stop_id

)
SELECT
    *
FROM count_rows
WHERE num_rows > 1