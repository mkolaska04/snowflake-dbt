{{ config(
    target_lag='1 MINUTE'
) }}

select
  t.*,
  ru.AGE,
  ru.GENDER,
  rp.CATEGORY
from {{ source('poc2', 'raw_transactions') }} t
left join {{ ref('ranked_user') }} ru
  on t.ID = ru.tid
 and t.VALID_FROM = ru.t_valid_from
 and t.TO_USER = ru.TO_USER
 and ru.rn = 1
left join {{ ref('ranked_product') }} rp
  on t.ID = rp.tid
 and t.VALID_FROM = rp.t_valid_from
 and t.PRODUCT_ID = rp.PRODUCT_ID
 and rp.rn = 1
