


WITH trip_delay as (
    SELECT
        route_id,
        start_time,
        direction_id,
        MAX(stop_delay) as max_delay

    FROM {{ ref('latest_data') }}
    GROUP BY route_id, start_time, direction_id
    ),

    ordered_route as (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY route_id, start_time, direction_id ORDER BY max_delay DESC) as route_rank
        FROM trip_delay

    ),

    worst_routes as (
        SELECT
            route_id,
            start_time,
            direction_id,
            max_delay
        FROM ordered_route
        WHERE route_rank <= 10
    )

SELECT
    route_id,
    AVG(max_delay) as avg_delay
FROM worst_routes
GROUP BY route_id
ORDER BY avg_delay DESC
LIMIT 10