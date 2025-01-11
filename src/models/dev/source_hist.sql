{{
  config(
    materialized = 'table',
    )
}}

select *
, current_timestamp as ingested_at
from {{ source('de_project_teachers', 'historical_stop_times') }}