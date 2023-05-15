	
{{ config(
    tags=["staging","promotions","details"]
) }}

with 
stg_details_manufacturer as (
	select 
	item_key,
	detail,
    lower(detail_value)	"manufacturer"
	from {{ ref('staging_details') }}
	where detail = ('Fabricante')
)

select * from stg_details_manufacturer
