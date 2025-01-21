  {{
  config(
    materialized = 'table',
    )
}}

SELECT DISTINCT
  unnested_field.dataPrevisao as weather_date
, unnested_field.tMin as min_temperature
, unnested_field.tMax as max_temperature
, unnested_field.rumoPredVento as wind_direction
, unnested_field.probPrecipita as rain_probability
, {{ var('truncate_timespan_to') }}  as ingested_at
FROM {{ source('data_eng_project_group2', 'api_weather_forecast_cleaned') }},
UNNEST(data) as unnested_field