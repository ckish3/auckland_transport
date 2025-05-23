-- This table adds columns of transformations of the response time (hour, day of the week, etc.)

SELECT
    *,
    date_part('hour', to_timestamp(epoch_time)) as response_hour,
    EXTRACT(DOW FROM to_timestamp(epoch_time)) as day_of_week,
    CASE WHEN EXTRACT(DOW FROM to_timestamp(epoch_time)) = 0 OR EXTRACT(DOW FROM to_timestamp(epoch_time)) = 6 THEN 'weekend'
        ELSE 'weekday' END as day_type,
    to_timestamp(epoch_time) as response_datetime,
    date_trunc('day', to_timestamp(epoch_time))::DATE as response_date
FROM {{ ref('trip_update') }}