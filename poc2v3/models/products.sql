
select
  p.*
from {{ source('poc2', 'raw_products') }} as p


  