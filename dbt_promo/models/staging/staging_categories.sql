with
staging_categories as (
  select 
  item_key,
  hierarchy,
  trim(category) "category_name"
  from 
   {{ source('promo_details','raw_categories') }}
),
staging_categories_1 as (
  select 
  k.detail_value as "item_key",
  hierarchy "item_hierarchy" ,
  category_name  
  from staging_categories c
left join {{ ref('staging_details_key')}} k on c.item_key = k.item_key 
),
staging_categories_2 as (
  select 
  {{ dbt_utils.generate_surrogate_key(['i.item_surrogate_key', 'category_name']) }} "item_category_key",
  i.item_surrogate_key,
  c.item_key,
  item_hierarchy,
  category_name
  from 
  staging_categories_1 c join 
 {{ ref('staging_items') }} i on c.item_key = i.item_key 

)

select * from staging_categories_2