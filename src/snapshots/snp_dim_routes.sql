{% snapshot snp_dim_routes %}
{{
  config(
    target_schema='data_eng_project_group2_snapshots',
    unique_key='sk_route',
    strategy='check',
    check_cols=['route_id', 'route_name', 'municipalities', 'stops']
  )
}}

SELECT 
    sk_route,
    route_id,
    line_id,
    route_name,
    ARRAY_TO_STRING(
        ARRAY(
            SELECT TO_JSON_STRING(STRUCT(
                CAST(municipality_id AS STRING) AS municipality_id,
                municipality_name AS municipality_name
            ))
            FROM UNNEST(municipalities)
            ORDER BY municipality_id
        ),
        ','
    ) AS municipalities,
    ARRAY_TO_STRING(
        ARRAY(
            SELECT TO_JSON_STRING(STRUCT(
                CAST(stop_id AS STRING) AS stop_id,
                stop_name AS stop_name,
                region_name AS region_name,
                stop_operational_status AS stop_operational_status
            ))
            FROM UNNEST(stops)
            ORDER BY stop_id
        ),
        ','
    ) AS stops,
    ingested_at
FROM {{ ref('dim_routes') }}

{% endsnapshot %}