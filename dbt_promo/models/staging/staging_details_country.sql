	
{{ config(
    tags=["staging","promotions","details"]
) }}

{{ standarize('staging.staging_country','detail_value','staging_details_country','staging_country') }}


select * from standarized_table