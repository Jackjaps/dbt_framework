with
staging_promo_c1 as (

  select 
  item_key,
  item_name,
  {{ dbt.replace("price", "'$'", "''") }} as "price",
  {{ dbt.replace("discount", "'%'", "''") }} as "discount",
  {{ dbt.safe_cast("ext_date", api.Column.translate_type("date")) }} as ext_date 
  from 
   {{ source('promo_details','raw_promotion_details') }}

),
staging_promo_c2 as (
  select 
  item_key,
  item_name,
  {{ dbt.replace("price", "','", "''") }} as "price",
  {{ dbt.replace("discount", "'{'", "''") }} as "discount",
  ext_date
  from 
    staging_promo_c1
),
staging_promo_c3 as (
  select 
  item_key,
  item_name,
  {{ dbt.replace("price", "'..'", "''") }} as "price",
  {{ dbt.replace("discount", "'}'", "''") }} as "discount",
  ext_date
  from 
    staging_promo_c2
),
staging_promo_c4 as (
  select 
  item_key,
  item_name,
  regexp_replace(price, '\r|\n', '') as "price",
  regexp_replace(discount, '\r|\n', '')  as "discount",
  ext_date
  from 
    staging_promo_c3
),
staging_promo_c5 as (
  select 
  item_key,
  item_name,
  price,
  {{ dbt.replace("discount", "'$'", "''") }} as "discount",
  ext_date
  from 
    staging_promo_c4
)

select 

* 

from staging_promo_c5

