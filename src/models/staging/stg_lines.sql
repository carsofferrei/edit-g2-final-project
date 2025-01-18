  {{
  config(
    materialized = 'view',
    )
}}

SELECT DISTINCT
            id AS line_id,
            long_name AS line_name,
            route_id AS route_id
        FROM {{ source('data_eng_project_group2', 'api_lines_cleaned') }}, 
            UNNEST(routes) AS route_id