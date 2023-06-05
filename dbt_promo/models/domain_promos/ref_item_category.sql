{{
  config(
    materialized='incremental',
    unique_key='item_category_key',
    merge_update_columns = ['item_surrogate_key','item_hierarchy', 'category_name']
  )
}}

  SELECT 
  item_category_key,
  item_surrogate_key,
  item_hierarchy,
  category_name,
  {{ dbt_date.now() }} created_dt
  FROM {{ ref('staging_categories') }}

