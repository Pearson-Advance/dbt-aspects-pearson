{#
    Parse a taxonomy-id var into a list of ints. Accepts a single id, a
    comma-separated string (the env-var form) or a list (the --vars form).
    Blank values and 0 are dropped, so an unconfigured var yields [].
#}
{% macro pss_taxonomy_ids(var_name) %}
    {%- set value = var(var_name) -%}
    {%- if value is string -%} {%- set items = value.split(",") -%}
    {%- elif value is iterable -%} {%- set items = value -%}
    {%- else -%} {%- set items = [value] -%}
    {%- endif -%}
    {%- set ids = [] -%}
    {%- for item in items -%}
        {%- set text = item | string | trim -%}
        {%- if text != "" and (text | int) != 0 -%}
            {%- do ids.append(text | int) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(ids) }}
{% endmacro %}

{#
    SQL Array(Int32) literal for has(...) filters. The cast keeps an empty
    list valid in ClickHouse, where the filter then matches nothing.
#}
{% macro pss_taxonomy_ids_array(var_name) %}
    cast([{{ pss_taxonomy_ids(var_name) | join(", ") }}] as Array(Int32))
{% endmacro %}

{# SQL list of the accepted activity types, e.g. ('Quiz', 'Lab', ...). #}
{% macro pss_activity_types_sql() %}
    ('{{ var("pss_activity_types") | join("', '") }}')
{% endmacro %}
