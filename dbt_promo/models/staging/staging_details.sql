with
staging_cleansing as (
  select 
  item_key,
  {{ dbt.replace("detail", "':'", "''") }} as "detail",
  trim(value) "detail_value"
  from 
   {{ source('promo_details','raw_details') }}
),
staging_cleansing_trim as (
  select 
  item_key,
  trim(detail) "detail",
  detail_value
  from 
   staging_cleansing
)
select * from staging_cleansing_trim