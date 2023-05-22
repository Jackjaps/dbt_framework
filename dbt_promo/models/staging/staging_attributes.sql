with
staging_attributes_dq as (
  select 
  item_key,
  trim(attribute) "attribute_name",
  trim(value) "attribute_value"
  from 
   {{ source('promo_details','raw_attributes') }}

),
staging_attributes_key as (
  select 
    k.detail_value "item_key",
    a.attribute_name,
    a.attribute_value
  from 
    staging_attributes_dq a
  join {{ ref('staging_details_key') }} k on a.item_key = k.item_key
),
staging_attributes_final as ( 

select 
  {{ dbt_utils.generate_surrogate_key(['i.item_surrogate_key', 'attribute_name','attribute_value']) }} "item_attribute_key",
  i.item_surrogate_key,
  a.item_key,
  attribute_name,
  attribute_value
  from staging_attributes_key a join 
 {{ ref('staging_items') }} i on a.item_key = i.item_key 
)


select * from staging_attributes_final

