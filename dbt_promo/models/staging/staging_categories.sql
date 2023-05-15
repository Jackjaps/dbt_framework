with
staging_categories as (
  select 
  item_key,
  hierarchy,
  trim(category) "category_name"
  from 
   {{ source('promo_details','raw_categories') }}
)

select * from staging_categories