{% snapshot fact_promotions %}

{{
    config(
      target_database='postgres',
      target_schema='model',
      unique_key='item_surrogate_key',
      strategy='timestamp',
      updated_at='extraction_dt',
    )
}}

select 
* 
from 
{{ ref('final_staging_promotions') }}


{% endsnapshot %}