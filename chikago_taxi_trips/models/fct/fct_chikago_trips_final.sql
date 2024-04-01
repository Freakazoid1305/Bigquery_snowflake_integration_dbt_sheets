{{
config(
materialized = 'incremental',
on_schema_change='fail'
)
}}


SELECT 
    all_trips_tips.taxi_id,
    all_trips_tips.year_month,
    all_trips_tips.total_tips AS tips_sum,
    CASE 
        WHEN all_trips_tips.tips_prev_month <> 0 THEN
            ROUND((all_trips_tips.total_tips - all_trips_tips.tips_prev_month) / all_trips_tips.tips_prev_month * 100, 2)
        ELSE
            NULL
    END AS tips_change
FROM 
    {{ ref ('fct_chikago_trips_all_trips_tips') }} all_trips_tips
INNER JOIN 
    {{ ref ('fct_chikago_trips_top3') }} top_taxis ON all_trips_tips.taxi_id = top_taxis.taxi_id

{% if is_incremental() %}
AND all_trips_tips.year_month > (select max(all_trips_tips.year_month) from {{ this }})
{% endif %}
