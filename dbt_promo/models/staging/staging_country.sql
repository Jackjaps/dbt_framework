with 
stg_details_country as (
	select 
	item_key,
	detail,
	detail_value
	from {{ ref('staging_details') }}
	where detail = ('País de origen')
)

select * from stg_details_country