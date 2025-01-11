{{
  config(
    materialized = 'table',
    )
}}

WITH DATES as ({{ dbt_date.get_date_dimension("2015-01-01", "2099-12-31") }})

SELECT *
, current_timestamp as ingested_at
FROM DATES 