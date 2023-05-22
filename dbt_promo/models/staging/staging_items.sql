
select 
    {{ dbt_utils.generate_surrogate_key(['item_key', 'item_name']) }} "item_surrogate_key",
	item_key,
	item_name,
	item_model,
	item_language,
	item_manufacturer,
	item_country,
	release_dt,
	available_dt
from 
{{ ref('staging_promotions') }}
where item_key not in 
( 
    select item_key
    from
    {{ ref('staging_promotions') }} group by item_key having count(*)>1
)