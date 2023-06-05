
select 
        item_surrogate_key,
        item_price,
		item_discount,
        (item_price*100)/(100 - abs(item_discount)) original_price,
		item_rankings,
		item_results,
		extraction_dt
from 
{{ ref('staging_promotions') }}
where item_key not in 
( 
    select item_key
    from
    {{ ref('staging_promotions') }} group by item_key having count(*)>1
)
and item_price > 0 