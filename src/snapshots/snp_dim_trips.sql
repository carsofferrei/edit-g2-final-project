{% snapshot snp_dim_trips %}
{{
  config(
     target_schema='data_eng_project_group2_snapshots',
     unique_key='sk_trip',
     strategy='check',
     check_cols=['line_id', 'line_name', 'route_id', 'route_name', 'direction_id', 'direction_name']
   )
}}

SELECT * 
FROM {{ ref('dim_trips') }}

{% endsnapshot %}