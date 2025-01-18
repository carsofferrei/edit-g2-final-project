  {{
  config(
    materialized = 'view',
    )
}}

SELECT trip_id,
    route_id,
    service_id,
    direction_id,
    CASE WHEN direction_id = 0 THEN 'Outbound Travel'
          WHEN direction_id = 1 THEN 'Inbound Travel'
      ELSE 'N/A'
      END AS direction_name
FROM {{ source('data_eng_project_group2', 'trips') }}