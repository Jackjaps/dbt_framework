{{
  config(
    materialized='incremental',
    unique_key='item_surrogate_key',
    incremental_strategy = 'delete+insert'
  )
}}

{{ dimensions('staging_items',["item_surrogate_key","item_key"],["item_name","item_model","item_language","item_manufacturer","item_country"]) }}
