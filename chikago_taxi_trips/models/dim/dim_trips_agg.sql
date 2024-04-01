WITH src_trips AS (
    SELECT * 
    FROM {{ref ('src_chikago_trips') }}
)

SELECT taxi_id,
    trip_date,
    SUM(tips) AS total_tips
FROM
    src_trips
WHERE TRIP_DATE >= '2018-04-01 00:00:00'
GROUP BY 1, 2
ORDER BY 3 DESC