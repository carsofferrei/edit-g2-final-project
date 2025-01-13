{{
  config(
    materialized = 'view'
  )
}}

WITH API_STOP AS (
  SELECT
    id
    , lat
    , lon
    , stop_id
    , CAST(line_id AS STRING) AS lines_id
  FROM 
    {{ source('data_eng_project_group2', 'api_stops') }},
    UNNEST(lines) AS line_id
)

, HIST_STOP_TIMES AS (
  SELECT
      *
    , TIMESTAMP_SECONDS(timestamp) AS timestamp_UTC
    , DATE(TIMESTAMP_SECONDS(timestamp)) AS trip_dat
  FROM 
      {{ source('de_project_teachers', 'historical_stop_times') }}
)

, JOIN_STOPS_HIST AS (
  SELECT 
    DISTINCT A.*
    , B.lat
    , B.lon
    , ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY timestamp_UTC) AS stop_sequence
  FROM HIST_STOP_TIMES A
  LEFT JOIN API_STOP B
  ON A.stop_id = B.stop_id AND CAST(A.line_id AS STRING) = B.lines_id
)

, DISTANCE AS (
  SELECT
    trip_id
    , stop_id
    , line_id
    , stop_sequence
    , current_status
    , trip_dat
    , lat
    , lon
    , LAG(lat) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS prev_lat
    , LAG(lon) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS prev_lon
  FROM JOIN_STOPS_HIST
)

, DISTANCE_FINAL AS (
  SELECT
      trip_id
      , stop_sequence
      , stop_id
      , line_id
      , current_status
      , trip_dat
      , ROUND(
        CASE 
          WHEN prev_lat IS NOT NULL AND prev_lon IS NOT NULL 
          THEN ST_DISTANCE(ST_GEOGPOINT(prev_lon, prev_lat), ST_GEOGPOINT(lon, lat)) / 1000
          ELSE 0
          END, 3) AS distance_between_stops_km
      , ROUND(
          SUM(CASE 
                WHEN prev_lat IS NOT NULL AND prev_lon IS NOT NULL 
                THEN ST_DISTANCE(ST_GEOGPOINT(prev_lon, prev_lat), ST_GEOGPOINT(lon, lat)) / 1000
                ELSE 0
                END) OVER (PARTITION BY trip_id ORDER BY stop_sequence)
              , 3) AS cumulative_distance_km
  FROM DISTANCE
  ORDER BY trip_id, stop_sequence
)

, HIST_STOP_TIMES_TRIP AS (
  SELECT
    line_id
    , trip_id
    , trip_dat
    , MIN(timestamp_UTC) AS begin_trip
    , MAX(timestamp_UTC) AS end_trip
    , COUNT(DISTINCT stop_id) AS trip_stops
  FROM HIST_STOP_TIMES
  GROUP BY line_id, trip_id, trip_dat
)

, AUX_FINAL AS (
  SELECT 
      DISTANCE_FINAL.trip_id
    , DISTANCE_FINAL.line_id
    , DISTANCE_FINAL.trip_dat
    , HIST_STOP_TIMES_TRIP.trip_stops
    , MAX(cumulative_distance_km) AS trip_total_distance_km
    , TRUNC(TIMESTAMP_DIFF(end_trip, begin_trip, SECOND) / 60) AS trip_total_time_min
  FROM DISTANCE_FINAL
  LEFT JOIN HIST_STOP_TIMES_TRIP
  ON DISTANCE_FINAL.trip_id = HIST_STOP_TIMES_TRIP.trip_id AND DISTANCE_FINAL.line_id = HIST_STOP_TIMES_TRIP.line_id
  GROUP BY DISTANCE_FINAL.trip_id, DISTANCE_FINAL.line_id, DISTANCE_FINAL.trip_dat, HIST_STOP_TIMES_TRIP.trip_stops, TRUNC(TIMESTAMP_DIFF(end_trip, begin_trip, SECOND) / 60) 
)

SELECT  
    AUX_FINAL.trip_id
    , line_id
    , direction_id
    , CASE WHEN direction_id = 0 THEN 'outbound travel' ELSE 'inbound travel' END AS direction_desc
    , route_id
    , trip_dat
    , trip_stops
    , trip_total_distance_km
    , trip_total_time_min
    , ROUND(CASE
            WHEN trip_total_time_min > 0 THEN trip_total_distance_km / trip_total_time_min
            ELSE NULL
            END, 3) AS trip_avg_speed
FROM AUX_FINAL
LEFT JOIN {{ source('data_eng_project_group2', 'api_trips') }} B
ON AUX_FINAL.trip_id = B.trip_id
