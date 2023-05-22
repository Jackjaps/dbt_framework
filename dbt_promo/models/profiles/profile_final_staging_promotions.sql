-- depends_on: {{ ref("final_staging_promotions") }}
{% if execute %}
    {{ dbt_profiler.get_profile(relation=ref("final_staging_promotions")) }}
{% endif %}