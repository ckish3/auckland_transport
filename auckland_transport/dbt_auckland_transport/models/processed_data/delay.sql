-- This table adds transformations of the delay estimates. It converts the delay in seconds to
--minutes, and categorises the delay as early, on time, 5-10 minutes late, or 10+ minutes late

SELECT
    *,
    stop_delay / 60.0 as minutes_delay,
    CASE WHEN stop_delay < -5*60 THEN 'early'
        WHEN stop_delay >= 10*60 THEN '10+ minute delay'
        WHEN stop_delay > 5*60 AND stop_delay < 10*60 THEN '5-10 minute delay'
        ELSE 'on time' END as delay_category
FROM {{ ref('time_columns') }}