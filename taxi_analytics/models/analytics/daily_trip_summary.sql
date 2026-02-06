-- NYC Taxi Project/taxi_analytics/models/analytics/daily_trip_summary.sql
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['trip_date', 'pickup_location_id'], 'unique': False}
    ]
) }}

WITH daily_trips AS (
    SELECT
        DATE(pickup_datetime) AS trip_date,
        pickup_location_id,
        
        -- Trip counts
        COUNT(*) AS total_trips,
        
        -- Distance metrics
        ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
        ROUND(MIN(trip_distance)::numeric, 2) AS min_distance,
        ROUND(MAX(trip_distance)::numeric, 2) AS max_distance,
        
        -- Fare metrics
        ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
        ROUND(SUM(fare_amount)::numeric, 2) AS total_fare,
        
        -- Tip metrics
        ROUND(AVG(tip_amount)::numeric, 2) AS avg_tip,
        ROUND(AVG(tip_percentage)::numeric, 2) AS avg_tip_percentage,
        
        -- Revenue
        ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
        
        -- Passenger metrics
        ROUND(AVG(passenger_count)::numeric, 2) AS avg_passengers,
        
        -- Duration metrics
        ROUND(AVG(trip_duration_minutes)::numeric, 2) AS avg_duration_minutes
        
    FROM {{ ref('stg_taxi_trips') }}
    GROUP BY 1, 2
)

SELECT * FROM daily_trips
ORDER BY trip_date DESC, total_trips DESC