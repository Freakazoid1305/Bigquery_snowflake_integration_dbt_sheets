WITH all_trips_tips AS (
    SELECT 
        taxi_id,
        trip_date AS year_month,
        total_tips,
        LAG(total_tips) OVER (PARTITION BY taxi_id ORDER BY trip_date) AS tips_prev_month
    FROM 
        {{ ref('dim_trips_agg') }}
    WHERE 
        trip_date >= '2018-04-01' -- All months since april 2018
)

SELECT *
FROM all_trips_tips