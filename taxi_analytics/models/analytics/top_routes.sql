-- NYC Taxi Project/taxi_analytics/models/analytics/top_routes.sql
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['pickup_location_id', 'dropoff_location_id'], 'unique': False}
    ]
) }}

SELECT
    pickup_location_id,
    dropoff_location_id,
    
    -- Trip volume
    COUNT(*) AS trip_count,
    
    -- Distance metrics
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
    ROUND(MIN(trip_distance)::numeric, 2) AS min_distance,
    ROUND(MAX(trip_distance)::numeric, 2) AS max_distance,
    
    -- Financial metrics
    ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
    ROUND(SUM(fare_amount)::numeric, 2) AS total_fare,
    ROUND(AVG(tip_percentage)::numeric, 2) AS avg_tip_percentage,
    
    -- Time metrics (using pre-calculated field from staging)
    ROUND(AVG(trip_duration_minutes)::numeric, 2) AS avg_duration_minutes,
    ROUND(MIN(trip_duration_minutes)::numeric, 2) AS min_duration_minutes,
    ROUND(MAX(trip_duration_minutes)::numeric, 2) AS max_duration_minutes,
    
    -- Revenue
    ROUND(AVG(total_amount)::numeric, 2) AS avg_total_amount

FROM {{ ref('stg_taxi_trips') }}
WHERE 
    pickup_location_id != dropoff_location_id  -- Exclude same location trips
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
GROUP BY 1, 2
ORDER BY trip_count DESC
LIMIT 100