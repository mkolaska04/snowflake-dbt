{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='COMPUTE_WH',
    target_lag='downstream',
) }}

select
  ID,
  FIRST_NAME || ' ' || LAST_NAME AS NAME,
  EMAIL,
  AGE,
  GENDER,
  VALID_FROM
from {{ source('poc2', 'raw_users') }}
