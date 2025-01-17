
  {{
  config(
    materialized = 'view',
    )
}}


WITH 
    expanded_data AS ( -- Expands the arrays of ROUTES
        SELECT DISTINCT
            id AS line_id,
            long_name AS line_name,
            route_id AS route_id
        FROM {{ source('data_eng_project_group2', 'api_lines_cleaned') }}, 
            UNNEST(routes) AS route_id
    ),

    routes_joined AS (
        SELECT
            A.route_id,
            B.long_name AS route_name,
            A.line_id,
            A.line_name
        FROM expanded_data A
        LEFT JOIN {{ source('data_eng_project_group2', 'api_routes_cleaned') }} B 
            ON A.route_id = B.id
    ),

    final_result AS (
        SELECT 
            B.trip_id,
            A.line_id,
            A.line_name,
            A.route_id,
            A.route_name,
            B.direction_id
        FROM routes_joined A
        LEFT JOIN {{ source('data_eng_project_group2', 'trips') }} B 
            ON A.route_id = B.route_id
    )

SELECT *
FROM final_result
