-- depends_on: {{ ref("fact_promotions") }}
{% if execute %}
    {{ dbt_profiler.get_profile(relation=ref("fact_promotions")) }}
{% endif %}