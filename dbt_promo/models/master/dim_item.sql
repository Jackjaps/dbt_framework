{{
  config(
    materialized='incremental',
    unique_key='item_surrogate_key',
    merge_update_columns = ['item_name', 'item_model','item_language','item_manufacturer','item_country'],
    merge_exclude_columns = ['created_dt']
  )
}}

  SELECT 
  item_surrogate_key,
  item_key,
  item_name,
  item_model,
  item_language,
  item_manufacturer,
  item_country,
  {{ dbt_date.now() }} created_dt
  FROM {{ ref('staging_items') }}

