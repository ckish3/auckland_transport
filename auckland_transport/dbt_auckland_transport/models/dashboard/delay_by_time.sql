-- This table gives the average delay by hour of day

WITH weekday_avg as (
    SELECT
        response_hour,
        percentile_disc(0.5) within group (order by minutes_delay) as avg_delay
    FROM {{ ref('latest_data') }}
    WHERE day_type = 'weekday'
    GROUP BY response_hour
    ),
    weekend_avg as (
    SELECT
        response_hour,
        percentile_disc(0.5) within group (order by minutes_delay) as avg_delay
    FROM {{ ref('latest_data') }}
    WHERE day_type = 'weekend'
    GROUP BY response_hour
    )

SELECT
    w.response_hour::INT as response_hour,
    w.avg_delay as weekday_avg,
    e.avg_delay as weekend_avg
FROM weekday_avg w
LEFT JOIN weekend_avg e
ON w.response_hour = e.response_hour
ORDER BY w.response_hour ASC