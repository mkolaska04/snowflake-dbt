{{ config(
    materialized='ephemeral'
) }}

select
  t.ID as tid,
  t.VALID_FROM as t_valid_from,
  t.TO_USER,
  u.AGE,
  u.GENDER,
  row_number() over (
    partition by t.ID, t.VALID_FROM, t.TO_USER
    order by
      case when u.VALID_FROM <= t.DATE_OF_TRANSACTION then 0 else 1 end asc,
      case when u.VALID_FROM <= t.DATE_OF_TRANSACTION then u.VALID_FROM end desc nulls last,
      case when u.VALID_FROM >  t.DATE_OF_TRANSACTION then u.VALID_FROM end asc  nulls last
  ) as rn
from {{ source('poc2', 'raw_transactions') }} t
left join {{ ref('users') }} u
  on t.TO_USER = u.ID
