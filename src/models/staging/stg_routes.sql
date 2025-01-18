  {{
  config(
    materialized = 'view',
    )
}}

SELECT
  id as route_id,
  long_name as route_name,
  line_id,
  municipality_id,
  municipality_name
FROM {{ source('data_eng_project_group2', 'api_routes_cleaned') }}, 
  UNNEST(municipalities) AS municipality_id,
  UNNEST(localities) AS municipality_name