with 
stg_details_language_1 as (
	select 
	item_key,
	detail,
	detail_value
	from {{ ref('staging_details') }}
	where detail = ('Idioma')
)

select * from stg_details_language_1