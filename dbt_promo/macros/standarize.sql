{#
Standarize columns using reference table "staging_standarize" (this can be changed) in future implementations

Pre-requisites: 

CREATE TABLE schema.ref_table_<domain> (
	table_name varchar(50) NULL,
	column_name varchar(50) NULL,
	valid_value varchar(100) NULL,
	possible_value1 varchar(100) NULL,
	possible_value2 varchar(100) NULL,
	possible_value3 varchar(100) NULL,
	possible_value4 varchar(100) NULL,
	possible_value5 varchar(100) NULL,
	possible_value6 varchar(100) NULL,
	possible_value7 varchar(100) NULL,
	possible_value8 varchar(100) NULL,
	possible_value9 varchar(100) NULL
);

standarize (source_table_name,column_to_standarize,target_table_name,reference_table)

Example Usage: {{ standarize('staging_lang','detail_value','stg_details_language') }}

Arguments:
    Source_Table : Source table to standarize
    Column_to_standarize: The datatype to cast all unpivoted columns to, required.
    Target_table_name: Table Name where the data will return. required.
    Reference_table_name: Table Name
#}

{% macro standarize (source_table_name,column_to_standarize,target_table_name,reference_table) -%}
        {%- set source_table_name_ref = ref("{0}".format(source_table_name)) -%}
        {% set all_columns = adapter.get_columns_in_relation(
            source_table_name_ref
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
                {{source_table_name_ref}} s 
                left join ref_table ref 
                on s.{{ column_to_standarize }} = ref.value
                    and ref.table_name = '{{ target_table_name }}'
                    and ref.column_name = '{{ column_to_standarize }}'
                    and ref.value is not null 

        )
{%- endmacro %}
