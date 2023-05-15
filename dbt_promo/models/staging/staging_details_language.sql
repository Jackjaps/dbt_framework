	
{{ config(
    tags=["staging","promotions","details"]
) }}

{{ standarize('staging.staging_language','detail_value','stg_details_language','staging_lang') }}


select * from standarized_table