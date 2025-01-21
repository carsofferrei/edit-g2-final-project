  {{
  config(
    materialized = 'table',
    )
}}

{% set surrogate_key_columns = ['A.route_id'] %}

SELECT {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} as sk_route,
   A.route_id,
   A.line_id,
   ARRAY_AGG(STRUCT(
       CAST(A.municipality_id AS STRING) AS municipality_id,
            A.municipality_name AS municipality_name
   )) AS municipalities,
   A.route_name,
   ARRAY_AGG(STRUCT(
       CAST(B.stop_id AS STRING) AS stop_id,
            B.stop_name AS stop_name,
            B.region_name AS region_name,
            B.operational_status AS stop_operational_status
   )) AS stops,
   {{ var('truncate_timespan_to') }}  AS ingested_at
FROM {{ ref('stg_routes') }} A
LEFT JOIN {{ ref('stg_stops') }} B
  ON A.route_id = B.route_id
GROUP BY
  {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
  , A.route_id
  , A.line_id
  , A.route_name
  , {{ var('truncate_timespan_to') }} 