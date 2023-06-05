{{
  config(
    materialized='incremental',
    unique_key='item_attribute_key',
    merge_update_columns = ['item_surrogate_key','attribute_name','attribute_value']
  )
}}

SELECT 
  item_attribute_key,
  item_surrogate_key,
  attribute_name,
  attribute_value,
  {{ dbt_date.now() }} created_dt
FROM {{ ref('staging_attributes') }}
