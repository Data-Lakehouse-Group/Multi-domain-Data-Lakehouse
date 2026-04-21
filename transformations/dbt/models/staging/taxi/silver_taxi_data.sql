SELECT
    ---Main ID---
    VendorID                    AS vendor_id,

    ---Location and Datetime Details---
    tpep_pickup_datetime        AS pickup_datetime,
    tpep_dropoff_datetime       AS dropoff_datetime,
    PULocationID                AS pickup_location_id,
    DOLocationID                AS dropoff_location_id,
    
    -- Trip details
    passenger_count,
    trip_distance,
    RatecodeID                  AS rate_code_id,
    store_and_fwd_flag,

    ---Payment Details---
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,

    ---Derived Silver Columns---
    trip_duration_minutes,
    trip_speed_mph,
    pickup_hour,
    pickup_day_of_week,
    pickup_date,
    payment_method,
    day_name,
    is_weekend,

    ---Partioned On Columns---
    source_year    AS pickup_year,
    source_month   AS pickup_month,

    ---Audit Columns---
    source_file,
    ingested_at     AS bronze_ingested_at,
    processed_at    AS silver_processed_at
FROM read_parquet(
    's3://silver/taxi/yellow_tripdata/**/*.parquet',
    hive_partitioning = true
)
-- Apply Partitioning
WHERE source_year = {{ var('year', 2023) }}
  AND source_month = {{ var('month', 1) }}