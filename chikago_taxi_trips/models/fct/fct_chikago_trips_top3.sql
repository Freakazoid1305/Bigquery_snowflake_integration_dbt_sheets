WITH top_taxis AS (
    SELECT 
        taxi_id,
        trip_date,
        total_tips
    FROM (
        SELECT 
            taxi_id,
            trip_date,
            total_tips,
            ROW_NUMBER() OVER (ORDER BY total_tips DESC) AS rank
        FROM 
            {{ ref ('dim_trips_agg') }}
        WHERE 
            trip_date ='2018-04-01 00:00:00' 
    ) AS ranked_trips
    WHERE 
        rank <= 3
)

SELECT *
FROM top_taxis