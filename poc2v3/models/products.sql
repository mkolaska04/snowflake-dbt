{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='COMPUTE_WH',
    target_lag='downstream',
    on_configuration_change='apply'
) }}

select
  p.*
from {{ source('poc2', 'raw_products') }} as p
