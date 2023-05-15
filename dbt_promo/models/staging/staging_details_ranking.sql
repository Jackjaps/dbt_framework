	
{{ config(
    tags=["staging","promotions","details"]
) }}

with 
tmp_rankings_separate as (

    select 
    item_key ,
    UNNEST(REGEXP_SPLIT_TO_ARRAY(detail_value,'\r|\n')) "rankings"
    from {{ ref('staging_details') }}
    where detail in ('Opinión media de los clientes')
),

tmp_rankings_separate_1 as (
    select  
    item_key ,
    case when rankings like '%calificaciones%' 
    then 
        rankings 
    else null end as "rankings",
    case when rankings like '%estrellas%' 
    then rankings 
    else null end as "results"
    from tmp_rankings_separate
),

tmp_rankings_separate_2 as (
    select item_key,max(rankings) "rankings",max(results) "results"
    from 
    tmp_rankings_separate_1
    group by item_key
)

select * from tmp_rankings_separate_2