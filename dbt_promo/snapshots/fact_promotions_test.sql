{% snapshot fact_promotion_snapshot %}

{{
    config(
      target_database='postgres',
      target_schema='model',
      unique_key=['item_surrogate_key']
      strategy='timestamp',
      updated_at='extraction_dt',
    )
}}

  SELECT 
  item_surrogate_key,
        item_price,
		item_discount,
		item_rankings,
		item_results,
		extraction_dt,
        {{ dbt_date.now() }} created_dt
  FROM {{ ref('final_staging_promotions') }}
{% endsnapshot %}