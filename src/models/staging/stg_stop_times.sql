  {{
  config(
    materialized = 'view',
    )
}}

WITH STOPS AS (

SELECT 
    trip_id
    , stop_id
    , shape_dist_traveled
    , stop_sequence
    , ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence DESC) AS ID_MAX_STOP
FROM {{ source('data_eng_project_group2', 'stop_times') }}

)

, MAX_DIST AS (

    SELECT trip_id
    , CASE WHEN shape_dist_traveled >= 1000 
            THEN shape_dist_traveled / 1000
            ELSE shape_dist_traveled
            END AS distance_km
FROM STOPS
WHERE ID_MAX_STOP = 1

)

SELECT 
      STOPS.trip_id
    , stop_id
    , stop_sequence
    , distance_km
FROM STOPS
LEFT JOIN MAX_DIST
ON STOPS.trip_id = MAX_DIST.trip_id
