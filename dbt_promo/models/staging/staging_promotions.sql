
{{ config(
    tags=["staging","promotions","details"]
) }}

with 
stg_promotion as (
	select 
		ky.detail_value "item_key",
		det.item_name,
		det.price "item_price",
		{{ dbt.replace("det.discount", "','", "''") }} "item_discount",
		model.detail_value "item_model",
		trim(substring(ranking.rankings,1,POSITION(' ' in ranking.rankings))) "item_rankings",
		trim(substring(ranking.results,1,POSITION(' ' in ranking.results))) "item_results",
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
), 
cleansing_promotions_1 as (
	select 
	item_key,
	item_name,
	case when "item_price" = '.' then 0 
	else 
		{{ dbt.safe_cast("item_price", api.Column.translate_type("numeric(8,2)")) }}
	end "item_price",
	case when "item_discount" = '' then null
	else 
		{{ dbt.safe_cast("item_discount", api.Column.translate_type("integer")) }}
	end "item_discount",
	item_model,
	{{ dbt.replace("item_rankings", "','", "''") }} "item_rankings",
	{{ dbt.safe_cast("item_results", api.Column.translate_type("numeric(8,2)")) }} "item_results",
	item_language,
	item_manufacturer,
	item_country,
	extraction_dt,
	release_dt,
	available_dt,
	name_md5
	from 
	stg_promotion
),
cleansing_promotions_2 as (
	select 
	{{ dbt_utils.generate_surrogate_key(['item_key', 'item_name']) }} "item_surrogate_key",
	item_key,
	item_name,
	item_price,
	case when item_discount > 0 then 0 else item_discount end "item_discount",
	item_model,
	{{ dbt.safe_cast("item_rankings", api.Column.translate_type("numeric(8,2)")) }} "item_rankings",
	item_results,
	item_language,
	item_manufacturer,
	item_country,
	extraction_dt,
	release_dt,
	available_dt,
	name_md5
	from 
	cleansing_promotions_1

)

select * from cleansing_promotions_2
