{{
  config(
    materialized = 'view',
    )
}}

WITH API_STOP AS (
SELECT
    id, 
    lat, 
    lon, 
    CAST(line_id AS STRING) AS lines_id
  FROM 
    `data-eng-dev-437916.data_eng_project_group2.api_stops`,
    UNNEST(lines) AS line_id
)

, JOIN_STOPS_HIST AS (
  SELECT DISTINCT A.*
  , B.lat
  , B.lon
  , ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY timestamp) AS STOP_SEQUENCE
  FROM (SELECT DISTINCT A.* FROM `data-eng-dev-437916.de_project_teachers.historical_stop_times` A)  A
  LEFT JOIN (SELECT * FROM `data-eng-dev-437916.data_eng_project_group2.api_stops`, 
    UNNEST(lines) AS line_id) B
ON A.stop_id = B.stop_id AND CAST(A.line_id AS STRING)=B.line_id
WHERE A.trip_id = '2006_0_3|2|1|1100_5ZVUM'
) 
,
distances AS (
  SELECT
    trip_id,
    stop_id,
    line_id,
    stop_sequence,
    current_status,
    lat,
    lon,
    LAG(lat) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS prev_lat,
    LAG(lon) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS prev_lon
  FROM
    JOIN_STOPS_HIST
)

SELECT
  trip_id,
  stop_sequence,
  stop_id,
  line_id,
  current_status,
  IF(prev_lat IS NOT NULL AND prev_lon IS NOT NULL,
     ST_DISTANCE(ST_GEOGPOINT(prev_lon, prev_lat), ST_GEOGPOINT(lon, lat)) / 1000,
     0
  ) AS distance_between_stops_km,
  SUM(IF(prev_lat IS NOT NULL AND prev_lon IS NOT NULL,
          ST_DISTANCE(ST_GEOGPOINT(prev_lon, prev_lat), ST_GEOGPOINT(lon, lat)) / 1000,
          0
      )) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS cumulative_distance_km
FROM
  distances
ORDER BY
  trip_id, stop_sequence