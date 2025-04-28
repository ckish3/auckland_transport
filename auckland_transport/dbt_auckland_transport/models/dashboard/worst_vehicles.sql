
-- This table ranks the worst vehicles by the amount of delay over the average vehicle on the same route
-- during the same time of day

WITH route_avg as (
    SELECT
        route_id,
        direction_id,
        stop_id,
        day_type,
        response_hour,
        AVG(stop_delay) as avg_delay
    FROM {{ ref('latest_data') }}
    GROUP BY route_id, direction_id, stop_id, day_type, response_hour
    ),

    vehicle_route_avg as (
        SELECT
            route_id,
            direction_id,
            stop_id,
            day_type,
            response_hour,
            vehicle_id,
            AVG(stop_delay) as avg_delay
        FROM {{ ref('latest_data') }}
        GROUP BY route_id, direction_id, stop_id, day_type, response_hour, vehicle_id
    ),

    vehicle_difference as (
        SELECT
            v.route_id,
            v.direction_id,
            v.stop_id,
            v.day_type,
            v.response_hour,
            v.vehicle_id,
            v.avg_delay - r.avg_delay as vehicle_delay
        FROM vehicle_route_avg v
        LEFT JOIN route_avg r
            ON v.route_id = r.route_id
            AND v.direction_id = r.direction_id
            AND v.stop_id = r.stop_id
            AND v.day_type = r.day_type
            AND v.response_hour = r.response_hour
    ),

    vehicle_avg as (
        SELECT
            vehicle_id,
            AVG(vehicle_delay) as avg_delay
        FROM vehicle_difference
        GROUP BY vehicle_id
    )

SELECT
    v.vehicle_id,
    v.avg_delay
FROM vehicle_avg v
ORDER BY avg_delay DESC