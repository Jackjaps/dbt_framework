
{{ config(
    tags=["staging","promotions","details"]
) }}

with 
stg_promotion as (


	select 
	ky.detail_value "item_key",
	det.item_name,
	det.price "item_price",
	det.discount "item_discount",
	model.detail_value "item_model",
	ranking.rankings "item_rankings",
	ranking.results "item_results",
	case when lang.valid_value is not null then lang.valid_value else lang.detail_value end "item_language",
	manuf.manufacturer "item_manufacturer",
	country.detail_value "item_country",
	det.ext_date "extraction_dt",
	rel_dt.release_dt,
	av.available_dt,
	det.item_key "name_md5"
	from      {{ ref('staging_promo_details') }} det 
	join 	  {{ ref('staging_details_key') }}			  ky		on det.item_key = ky.item_key
	left join {{ ref('staging_details_available_date') }} av   		on det.item_key = av.item_key
	left join {{ ref('staging_details_country') }}		  country	on det.item_key = country.item_key
	left join {{ ref('staging_details_language') }}		  lang		on det.item_key = lang.item_key
	left join {{ ref('staging_details_manufacturer') }}	  manuf		on det.item_key = manuf.item_key
	left join {{ ref('staging_details_model') }}		  model 	on det.item_key = model.item_key
	left join {{ ref('staging_details_ranking') }}		  ranking	on det.item_key = ranking.item_key
	left join {{ ref('staging_details_release_date') }}	  rel_dt	on det.item_key = rel_dt.item_key

)

select * from stg_promotion