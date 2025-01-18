  {{
  config(
    materialized = 'view',
    )
}}

SELECT PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS date
, service_id
, day_type
, exception_type
, holiday
, period
, CASE WHEN period = 1 THEN 'Período Escolar'
       WHEN period = 2 THEN 'Férias Escolares'
       WHEN period = 3 THEN 'Verão'
  ELSE 'N/A'
  END AS period_name
FROM {{ source('data_eng_project_group2', 'calendar_dates') }}