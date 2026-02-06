-- -- NYC Taxi Project/taxi_analytics/models/staging/stg_taxi_trips.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_taxi_trips') }}
),

-- Add row numbers to ensure uniqueness
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                tpep_pickup_datetime, 
                tpep_dropoff_datetime, 
                "PULocationID",
                "DOLocationID",
                passenger_count, 
                fare_amount, 
                trip_distance
            ORDER BY tpep_pickup_datetime
        ) AS row_num
    FROM source
    WHERE 
        -- Data quality filters
        tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND tpep_dropoff_datetime > tpep_pickup_datetime  -- Logical order
        AND fare_amount > 0
        AND fare_amount < 1000  -- Remove extreme outliers
        AND trip_distance > 0
        AND trip_distance < 100  -- Remove data errors
        AND passenger_count > 0
        AND passenger_count <= 6  -- Reasonable passenger limit
        AND total_amount > 0
),

cleaned AS (
    SELECT
        -- Generate unique trip ID with location IDs included
        {{ dbt_utils.generate_surrogate_key([
            'tpep_pickup_datetime', 
            'tpep_dropoff_datetime', 
            '"PULocationID"',
            '"DOLocationID"',
            'passenger_count', 
            'fare_amount',
            'trip_distance',
            'row_num'
        ]) }} AS trip_id,
        
        -- Timestamps
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        
        -- Calculate trip duration
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60 AS trip_duration_minutes,
        
        -- Location
        "PULocationID" AS pickup_location_id,
        "DOLocationID" AS dropoff_location_id,
        
        -- Trip details
        passenger_count,
        trip_distance,
        
        -- Fares
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        "Airport_fee" AS airport_fee,
        total_amount,
        
        -- Calculate tip percentage
        CASE 
            WHEN fare_amount > 0 
            THEN (tip_amount / fare_amount) * 100 
            ELSE 0 
        END AS tip_percentage,
        
        -- Payment
        payment_type,
        "VendorID" AS vendor_id,
        "RatecodeID" AS rate_code_id,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM deduplicated
    WHERE row_num = 1  -- Keep only first occurrence of duplicates
)

SELECT * FROM cleaned