{{ config(
    tags=["staging","promotions","details"]
) }}
-- depends_on: staging_country
{{ standarize('staging_country','detail_value','staging_details_country','staging_country') }}


select * from standarized_table