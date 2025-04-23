
SELECT
    *,
    date_part('hour', to_timestamp(epoch_time)) as hour,
    EXTRACT(DOW FROM to_timestamp(epoch_time)) as day_of_week,
    CASE WHEN EXTRACT(DOW FROM to_timestamp(epoch_time)) = 0 OR EXTRACT(DOW FROM to_timestamp(epoch_time)) = 6 THEN 'weekend'
        ELSE 'weekday' END as day_type
FROM {{ ref('trip_update') }}