
select *
from 
{{ ref('staging_promotions') }}
where item_key in 
( 
    select item_key
    from
    {{ ref('staging_promotions') }} group by item_key having count(*)>1
)