{{
  config(
    materialized = 'table',
    )
}}

-- Criar uma dim_date a partir de uma função dbt que está integrada numa package no dbt hubs
WITH DATES as ({{ dbt_date.get_date_dimension("2025-01-01", "2099-12-31") }})

SELECT *
, current_timestamp as ingested_at
FROM DATES 