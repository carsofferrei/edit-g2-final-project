  {{
  config(
    materialized = 'table',
    )
}}


WITH TRIPS AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['trip_id']) }} as sk_trip
    , {{ dbt_utils.generate_surrogate_key(['route_id']) }} as sk_route
    , trip_id 
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
  TRIPS.sk_trip
, TRIPS.sk_route
, STOP_TIMES.trip_id
, COALESCE(HIST_STOP_TIMES.trip_date, dates.date) AS trip_date
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