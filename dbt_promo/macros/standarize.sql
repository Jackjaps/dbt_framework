{#
Standarize columns using reference table "staging_standarize" (this can be changed) in future implementations

Example Usage: {{ standarize('staging.staging_lang','detail_value','stg_details_language') }}

Arguments:
    Reference Tabele name: Relation , required.
    column_to_standarize: The datatype to cast all unpivoted columns to, required.
    target_table_name: Table Name where the data will return. required.
#}

{% macro standarize (source_table_name,column_to_standarize,target_table_name,reference_table) -%}
        
        {% set all_columns = adapter.get_columns_in_relation(
            ref("staging_language")
        ) %}
        with ref_table as (
            {{dbt_utils.unpivot(
            relation=ref('staging_standarize'),
            exclude=["valid_value","table_name","column_name"]
            )}}
        ),
        standarized_table as (

            select 
            ref.valid_value,
            {%- for col in all_columns  %}  
                {{ col.name|lower }} {%- if not loop.last %},{{ '\n  ' }}{% endif %}
            {%- endfor %}
            from 
                {{source_table_name}} s left join 
                ref_table ref on s.{{ column_to_standarize }} = ref.value
                and ref.table_name = '{{ target_table_name }}'
                and ref.column_name = '{{ column_to_standarize }}'
                and ref.value is not null 

        )
{%- endmacro %}
