  {{
  config(
    materialized = 'view',
    )
}}

SELECT
    stop_id,
    name AS stop_name,
    municipality_id,
    municipality_name,
    region_name,
    operational_status,
    current_timestamp as ingested_at
FROM {{ source('data_eng_project_group2', 'api_stops_cleaned') }}