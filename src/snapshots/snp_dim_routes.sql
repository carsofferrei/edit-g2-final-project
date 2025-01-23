{% snapshot snp_dim_routes %}
{{
  config(
     target_schema='data_eng_project_group2_snapshots',
     unique_key='sk_route',
     strategy='check',
     check_cols=['route_id', 'municipalities', 'route_name', 'stops']
   )
}}

SELECT * 
FROM {{ ref('dim_routes') }}

{% endsnapshot %}