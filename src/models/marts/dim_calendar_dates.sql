{{
  config(
    materialized = 'table',
    )
}}

{% set surrogate_key_columns = ['date', 'service_id'] %}

SELECT 
{{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} as sk_date
, date
, service_id
, day_type
, exception_type
, holiday
, period
, period_name
FROM {{ ref('stg_calendar_dates') }}


