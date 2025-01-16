{{
  config(
    materialized = 'table',
    )
}}

WITH DIM_STOPS AS (

SELECT * 
FROM {{ ref('dim_stops') }}
WHERE UPPER(operational_status) LIKE 'ACTIVE'

)


SELECT
  A.trip_dat
, B.line_name
, B.route_name
, CASE WHEN B.direction_id = 0 THEN 'outbound travel' ELSE 'inbound travel' END AS direction_name
, C.stop_name
, C.municipality_name
, ROUND(AVG(A.trip_stops), 3) AS avg_trip_stops
, ROUND(AVG(A.trip_total_distance_km), 3) AS avg_total_distance_km
, ROUND(AVG(A.trip_total_time_min), 3) AS avg_total_time_min
, ROUND(AVG(A.trip_avg_speed), 3) AS avg_speed
FROM {{ ref('fact_historical_trips') }} A
INNER JOIN {{ ref('dim_lines') }} B
ON A.trip_id = B.trip_id
LEFT JOIN DIM_STOPS C
ON A.stop_id = C.stop_id
GROUP BY A.trip_dat
, B.line_name
, B.route_name
, CASE WHEN B.direction_id = 0 THEN 'outbound travel' ELSE 'inbound travel' END
, C.stop_name
, C.municipality_name