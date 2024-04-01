WITH raw_trips AS (
    SELECT * FROM {{source ('chikago_taxi_trips', 'chikago_taxi_trips') }}
)

SELECT 
    unique_key AS trip_id,
    taxi_id,
    DATE_TRUNC('month', trip_start_timestamp) AS trip_date,
    tips
FROM 
    raw_trips
WHERE 
    trip_start_timestamp >= CONVERT_TIMEZONE('UTC', 'America/Chicago', '2018-04-01 00:00:00')