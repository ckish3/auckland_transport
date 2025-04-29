-- This table is the last estimate for a stop on a trip

WITH

latest_updates AS (
    SELECT
        first_value(id) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as id,
        first_value(trip_id) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as trip_id,
        route_id,
        direction_id,
        start_time,
        start_date,
        stop_id,
        first_value(stop_time) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as stop_time,
        first_value(stop_delay) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as stop_delay,
        first_value(stop_uncertainty) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as stop_uncertainty,
        first_value(stop_sequence) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as stop_sequence,
        first_value(vehicle_id) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as vehicle_id,
        first_value(vehicle_license_plate) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as vehicle_license_plate,
        first_value(trip_delay) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as trip_delay,
        first_value(timestamp) over (partition by route_id, direction_id, start_time, start_date, stop_id  order by timestamp desc, id) as epoch_time


    FROM {{ source('raw_data', 'trip_update') }})

SELECT DISTINCT * FROM latest_updates
