-- window function
{{ config(materialized='table') }}

WITH trip_metrics AS (
    SELECT
        pickup_location_id,
        DATE_TRUNC('day', pickup_datetime) AS trip_date,
        COUNT(*) AS daily_trips,
        SUM(total_amount) AS daily_revenue,
        AVG(trip_distance) AS avg_distance
    FROM {{ ref('stg_taxi_trips') }}
    GROUP BY 1, 2
),

ranked_locations AS (
    SELECT
        *,
        -- Ranking functions 
        ROW_NUMBER() OVER (PARTITION BY trip_date ORDER BY daily_revenue DESC) AS revenue_rank,
        RANK() OVER (PARTITION BY trip_date ORDER BY daily_trips DESC) AS trip_volume_rank,
        DENSE_RANK() OVER (ORDER BY avg_distance DESC) AS distance_rank,
        
        -- Percentage of total 
        ROUND(100.0 * daily_revenue / SUM(daily_revenue) OVER (PARTITION BY trip_date), 2) AS pct_of_daily_revenue,
        
        -- Moving averages
        ROUND(AVG(daily_trips) OVER (
            PARTITION BY pickup_location_id 
            ORDER BY trip_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::numeric, 2) AS trips_7day_avg,
        
        -- LAG for day-over-day comparison
        LAG(daily_revenue) OVER (PARTITION BY pickup_location_id ORDER BY trip_date) AS prev_day_revenue,
        daily_revenue - LAG(daily_revenue) OVER (PARTITION BY pickup_location_id ORDER BY trip_date) AS revenue_change
        
    FROM trip_metrics
)

SELECT 
    trip_date,
    pickup_location_id,
    daily_trips,
    daily_revenue,
    revenue_rank,
    trip_volume_rank,
    pct_of_daily_revenue,
    trips_7day_avg,
    prev_day_revenue,
    ROUND(revenue_change::numeric, 2) AS revenue_change,
    CASE 
        WHEN revenue_change > 0 THEN 'Growth'
        WHEN revenue_change < 0 THEN 'Decline'
        ELSE 'Stable'
    END AS trend
FROM ranked_locations
WHERE revenue_rank <= 10  -- Top 10 locations by revenue each day
ORDER BY trip_date DESC, revenue_rank