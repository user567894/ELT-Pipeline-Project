-- NYC Taxi Project/taxi_analytics/models/analytics/hourly_demand.sql
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['trip_date', 'hour_of_day'], 'unique': False}
    ]
) }}

SELECT
    DATE(pickup_datetime) AS trip_date,
    EXTRACT(HOUR FROM pickup_datetime)::int AS hour_of_day,
    EXTRACT(DOW FROM pickup_datetime)::int AS day_of_week,  -- 0=Sunday, 6=Saturday
    
    -- Trip counts
    COUNT(*) AS trip_count,
    
    -- Financial metrics
    ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
    ROUND(SUM(fare_amount)::numeric, 2) AS total_fare,
    ROUND(AVG(tip_percentage)::numeric, 2) AS avg_tip_percentage,
    
    -- Trip characteristics
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
    ROUND(AVG(trip_duration_minutes)::numeric, 2) AS avg_duration_minutes,
    ROUND(AVG(passenger_count)::numeric, 2) AS avg_passengers,
    
    -- Revenue
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue

FROM {{ ref('stg_taxi_trips') }}
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2