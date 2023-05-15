
{{ config(
    tags=["staging","promotions","details"]
) }}

with 
stg_details_model as (
	select 
	item_key,detail,detail_value
	from {{ ref('staging_details') }}
	where detail = ('Número de modelo del producto')
)

select * from stg_details_model