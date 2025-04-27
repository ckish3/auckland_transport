--This table limits the final data for the dashboard to the last 365 days of data

SELECT
    *
FROM {{ ref('delay') }}
WHERE response_datetime >= DATE_ADD('day', -365, CURRENT_DATE) -- for a real dashboard, you would limit this to
--maybe 14 or 7 days, but given that this demonstration system runs infrequently, keep effectively
--all the data