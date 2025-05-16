--This test indicates whether the delay is in units larger than minutes. IT IS NOT POSSIBLE TO
--BE DEFINITIVE, SO A FAILURE DOES NOT MEAN THAT THE UNITS ARE DEFINITELY NOT MINUTES AND
--THIS TEST PASSING DOES NOT MEAN THAT THE UNITS ARE NOT IN HOURS

WITH avg_delay as (
    SELECT
        MAX(minutes_delay) as delay_time
    FROM {{ ref('delay') }}
)

SELECT
    *
FROM avg_delay
WHERE delay_time < 24