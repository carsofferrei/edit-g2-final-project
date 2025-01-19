  {{
  config(
    materialized = 'table',
    )
}}

{% set surrogate_key_columns = ['A.route_id'] %}

SELECT {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} as sk_route,
   A.route_id
  , A.line_id
  , ARRAY_AGG(CAST(A.municipality_id AS STRING)) as municipality_id
  , ARRAY_AGG(A.municipality_name) as municipality_name
  , A.route_name
  , ARRAY_AGG(CAST(B.stop_id AS STRING)) as stop_id
  , ARRAY_AGG(B.stop_name) as stop_name
  , ARRAY_AGG(B.region_name) as region_name
  , ARRAY_AGG(B.operational_status) AS stop_operational_status
  , current_timestamp as ingested_at
FROM {{ ref('stg_routes') }} A
LEFT JOIN {{ ref('stg_stops') }} B
  ON A.route_id = B.route_id 
GROUP BY 
  {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }}
  , A.route_id
  , A.line_id
  , A.route_name
  , current_timestamp