  {{
  config(
    materialized = 'table',
    )
}}

{% set surrogate_key_columns = ['route_id'] %}

WITH TRIPS AS (
    SELECT trip_id
    , {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} as sk_route
    , service_id
    , direction_name
FROM {{ ref('stg_trips') }}
)


, STOP_TIMES AS (
SELECT trip_id
      , MAX(ROUND(distance_km, 3)) AS total_trip_distance
      , MAX(stop_sequence) AS total_trip_stops
FROM {{ ref('stg_stop_times') }}
GROUP BY trip_id
)

, HIST_STOP_TIMES AS (
  SELECT trip_id
  , DATE(timestamp) AS trip_date
  , MIN(timestamp) AS start_trip
  , MAX(timestamp) AS end_trip
 FROM {{ source('de_project_teachers', 'historical_stop_times') }}
  GROUP BY trip_id, DATE(timestamp)
)


SELECT DISTINCT 
  STOP_TIMES.trip_id
, COALESCE(HIST_STOP_TIMES.trip_date, dates.date) AS trip_date
, TRIPS.sk_route
, STOP_TIMES.total_trip_distance
, STOP_TIMES.total_trip_stops
, TRUNC(TIMESTAMP_DIFF(HIST_STOP_TIMES.end_trip, HIST_STOP_TIMES.start_trip, SECOND) / 60) AS total_trip_time
, ROUND(CASE
            WHEN TRUNC(TIMESTAMP_DIFF(HIST_STOP_TIMES.end_trip, HIST_STOP_TIMES.start_trip, SECOND) / 60) > 0 
            THEN STOP_TIMES.total_trip_distance / TRUNC(TIMESTAMP_DIFF(end_trip, start_trip, SECOND) / 60)
            ELSE NULL
            END, 3) AS average_trip_speed
FROM STOP_TIMES
LEFT JOIN HIST_STOP_TIMES
ON STOP_TIMES.trip_id = HIST_STOP_TIMES.trip_id
LEFT JOIN TRIPS
ON STOP_TIMES.trip_id = TRIPS.trip_id
LEFT JOIN {{ ref('stg_calendar_dates') }} dates
ON TRIPS.service_id = dates.service_id