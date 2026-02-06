-- NYC Taxi Project/taxi_analytics/models/analytics/payment_analysis.sql
{{
    config(
        materialized='table'
    )
}}

WITH payment_stats AS (
    SELECT
        DATE(pickup_datetime) AS trip_date,
        CASE 
            WHEN payment_type = 1 THEN 'Credit Card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No Charge'
            WHEN payment_type = 4 THEN 'Dispute'
            ELSE 'Unknown'
        END AS payment_method,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_total_amount,
        AVG(fare_amount) AS avg_fare_amount,
        AVG(tip_amount) AS avg_tip_amount,
        AVG(CASE WHEN fare_amount > 0 THEN tip_percentage ELSE NULL END) AS avg_tip_percentage
    FROM {{ ref('stg_taxi_trips') }}
    GROUP BY 
        DATE(pickup_datetime),
        payment_type
)

SELECT 
    trip_date,
    payment_method,
    trip_count,
    ROUND(total_revenue::numeric, 2) AS total_revenue,
    ROUND(avg_total_amount::numeric, 2) AS avg_total_amount,
    ROUND(avg_fare_amount::numeric, 2) AS avg_fare_amount,
    ROUND(avg_tip_amount::numeric, 2) AS avg_tip_amount,
    ROUND(avg_tip_percentage::numeric, 2) AS avg_tip_percentage
FROM payment_stats
ORDER BY trip_date DESC, trip_count DESC