with
staging_attributes as (
  select 
  item_key,
  trim(attribute) "attribute_name",
  trim(value) "attribute_value"
  from 
   {{ source('promo_details','raw_attributes') }}

)

select * from staging_attributes