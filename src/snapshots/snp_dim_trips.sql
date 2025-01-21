{% snapshot snp_dim_trips %}
{{
  config(
     target_schema='data_eng_project_group2_snapshots',
     unique_key='sk_trip',
     strategy='check',
     check_cols=['line_id', 'line_name', 'route_id', 'route_name', 'direction_id', 'direction_name']
   )
}}

{% set surrogate_key_columns = ['trip_id'] %}

WITH routes_joined AS (
    SELECT 
        A.route_id,
        B.route_name,
        A.line_id,
        A.line_name
    FROM {{ ref('stg_lines') }} A
    LEFT JOIN {{ ref('stg_routes') }} B 
      ON A.route_id = B.route_id
),

final_result AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} AS sk_trip,
        B.trip_id,
        B.service_id,
        A.line_id,
        A.line_name,
        A.route_id,
        A.route_name,
        B.direction_id,
        B.direction_name
    FROM routes_joined A
    LEFT JOIN {{ ref('stg_trips') }} B 
      ON A.route_id = B.route_id
)

SELECT DISTINCT *
      , {{ var('truncate_timespan_to') }}  as ingested_at
FROM final_result


{% endsnapshot %}