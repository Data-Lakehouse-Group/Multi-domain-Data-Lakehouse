--- Adds the borough names for picup and dropoff
WITH 
    trips AS (
        SELECT * FROM {{ ref('silver_taxi_data') }}
    ),

    zones AS (
        SELECT
            LocationID,
            Borough  AS zone_borough,
            Zone     AS zone_name,
            service_zone
        FROM {{ ref('taxi_zone_lookup') }}
    ),

    joined AS (
        SELECT
            t.*,
            pu.zone_borough     AS pickup_borough,
            pu.zone_name        AS pickup_zone,
            pu.service_zone     AS pickup_service_zone,
            dro.zone_borough     AS dropoff_borough,
            dro.zone_name        AS dropoff_zone

        FROM trips t
        LEFT JOIN zones pu ON t.pickup_location_id  = pu.LocationID
        LEFT JOIN zones dro ON t.dropoff_location_id = dro.LocationID
    )
SELECT * FROM joined