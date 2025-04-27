SELECT
    *
FROM {{ ref('delay') }}
WHERE response_datetime >= DATE_ADD('day', -14, CURRENT_DATE)