{#

Dimensions macro inserts new records and update changed registries by hash check, if any column with in the hash change the hash will be different and will update only changed columns (no key columns)

Pre-requisites: Staging table with the delta of the information to be checked 

dimensions (source_table_name,[key column lists],[hash columns])

Example Usage: {{ dimensions('staging_items',["item_surrogate_key","item_key"],["item_name","item_model","item_language","item_manufacturer","item_country"]) }}

Arguments:
    source_table_name : Source table of the staging table dimensions, required.
    [key column lists]: List of key columns, required.
    [hash columns]: List of hash key, required.
#}

{% macro dimensions (table_name,key_columns,hash_columns) -%}
{%- set source_table = ref("{0}".format(table_name)) -%}
{%- set all_columns = adapter.get_columns_in_relation(
            source_table
        ) -%}
{%- if not is_incremental() -%}
select 
{%- for select_column in all_columns -%}
    {%- set ns = namespace(found=false) -%}
    {%- for k_column in key_columns -%}
    {%- if select_column.name == k_column -%}
    {%- set ns.found = true -%}
    {{ select_column.name }},{{'\n'}}
    {%- endif -%}
    {%- endfor -%}
{%- endfor -%}
{%- for select_column in all_columns -%}
    {%- set ns = namespace(found=false) -%}
    {%- for k_column in key_columns -%}
    {%- if select_column.name == k_column %}
        {%- set ns.found = true -%}
    {%- endif -%}
    {%- endfor -%}
    {%- if ns.found == false -%} 
    {{select_column.name}},{{'\n'}}
    {%- endif -%}
{%- endfor -%}
md5(concat(
{%- for select_column in all_columns -%}
    {%- set ns = namespace(found=false) -%}
    {%- for h_column in hash_columns -%}
    {%- if select_column.name == h_column -%}
    {%- set ns.found = true -%}
    {{ select_column.name }} {%- if not loop.last -%},{%- endif -%}
    {%- endif -%}
    {%- endfor -%}
{%- endfor -%})) hash_scd,
{{ dbt_date.now() }} created_dt
from 
  {{source_table}}
{% else %}
with source as (
select 
{% for select_column in all_columns -%}
    {%- set ns = namespace(found=false) -%}
    {%- for k_column in key_columns -%}
    {%- if select_column.name == k_column -%}
    {%- set ns.found = true -%}
    {{ select_column.name }},{{ '\n  ' }}
    {%- endif -%}
    {%- endfor -%}
{%- endfor -%}
{%- for select_column in all_columns -%}
    {%- set ns = namespace(found=false) -%}
    {%- for k_column in key_columns -%}
    {%- if select_column.name == k_column -%}
        {%- set ns.found = true -%}
    {%- endif -%}
{%- endfor -%}
{%- if ns.found == false -%} 
    {{select_column.name}},{{ '\n  ' }}
{%- endif -%}
{%- endfor %}
md5(concat(
{%- for select_column in all_columns -%}
    {%- set ns = namespace(found=false) %}
    {%- for h_column in hash_columns -%}
    {%- if select_column.name == h_column -%}
    {%- set ns.found = true -%}
    {{ select_column.name }} {%- if not loop.last -%},{%- endif -%}
    {%- endif -%}
    {%- endfor -%}
{%- endfor %}
)) hash_scd,
{{ dbt_date.now() }} created_dt
from
{{source_table}}
),
tar as (
select 
{% for select_column in all_columns -%}
    {%- set ns = namespace(found=false) -%}
    {%- for k_column in key_columns -%}
    {%- if select_column.name == k_column -%}
    {%- set ns.found = true -%}
        t.{{ select_column.name }},{{ '\n  ' }}
    {%- endif -%}
    {%- endfor -%}
{%- endfor %}
{%- for select_column in all_columns -%}
    {%- set ns = namespace(found=false) -%}
    {%- for k_column in key_columns -%}
    {%- if select_column.name == k_column -%}
        {%- set ns.found = true -%}
    {%- endif -%}
    {%- endfor -%}
{%- if ns.found == false -%} 
    s.{{select_column.name}},{{ '\n  ' }}
{%- endif -%}
{%- endfor -%}
s.hash_scd,
t.created_dt
from {{ this }} t join source s on 
          {% for select_column in all_columns -%}
                {%- set ns = namespace(found=false) %}
                {%- for k_column in key_columns -%}
                    {%- if select_column.name == k_column -%}
                        {%- set ns.found = true -%}
                        t.{{ select_column.name }} = s.{{ select_column.name }} {%- if not loop.last -%}{{' and '}}{%- endif -%}
                    {%- endif%}
                {%- endfor -%}
          {%- endfor %}  
          and t.hash_scd != s.hash_scd
        )
        select * from tar
      {%- endif -%}
{%- endmacro %}
