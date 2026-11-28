{{ config(tags=["pss_insights", "pss_insights_subdomains"]) }}

{% set scope = var(
    "pss_quiz_taxonomies_by_master",
    env_var("PSS_QUIZ_TAXONOMIES_BY_MASTER", "{}"),
) %}
{% if scope is string %} {% set scope = fromjson(scope) %} {% endif %}
{% if scope is not mapping %}
    {{
        exceptions.raise_compiler_error(
            "pss_quiz_taxonomies_by_master must map master course keys to lists of taxonomy IDs"
        )
    }}
{% endif %}
{% for master, ids in scope.items() %}
    {% if master is not string or ids is not sequence or ids is string or ids is mapping %}
        {{
            exceptions.raise_compiler_error(
                "Each Quiz taxonomy scope requires a master course key and a list of taxonomy IDs"
            )
        }}
    {% endif %}
    {% for id in ids %}
        {% if id is not integer or id <= 0 or id > 2147483647 %}
            {{
                exceptions.raise_compiler_error(
                    "Quiz taxonomy IDs must be positive Int32 values"
                )
            }}
        {% endif %}
    {% endfor %}
{% endfor %}

{% set encoded = tojson(scope) | replace("\\", "\\\\") | replace("'", "''") %}

select
    master_course,
    arrayJoin(
        arrayDistinct(
            JSONExtract('{{ encoded }}', assumeNotNull(master_course), 'Array(Int32)')
        )
    ) as taxonomy_id
from {{ ref("int_pss_master_course_scope") }}
