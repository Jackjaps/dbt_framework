
{{ config(
    tags=["staging","promotions","details"]
) }}

with 
stg_details_key as (
	select 
	item_key,detail,detail_value
	from {{ ref('staging_details') }}
	where detail = ('ASIN')
)

select * from stg_details_key