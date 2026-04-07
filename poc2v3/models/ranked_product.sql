{{ config(
    materialized='ephemeral'
) }}

select
  t.ID as tid,
  t.VALID_FROM as t_valid_from,
  t.PRODUCT_ID,
  p.CATEGORY,
  row_number() over (
    partition by t.ID, t.VALID_FROM, t.PRODUCT_ID
    order by
      case when p.VALID_FROM <= t.DATE_OF_TRANSACTION then 0 else 1 end asc,
      case when p.VALID_FROM <= t.DATE_OF_TRANSACTION then p.VALID_FROM end desc nulls last,
      case when p.VALID_FROM >  t.DATE_OF_TRANSACTION then p.VALID_FROM end asc  nulls last
  ) as rn
from {{ source('poc2', 'raw_transactions') }} t
left join {{ ref('products') }} p
  on t.PRODUCT_ID = p.ID
