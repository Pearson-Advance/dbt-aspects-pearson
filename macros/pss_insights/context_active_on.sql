{% macro pss_context_active_on(class_start, class_end, reporting_date) -%}
    if(
        {{ class_start }} is null
        or {{ class_end }} is null
        or {{ class_start }} > {{ class_end }},
        cast(null as Nullable(UInt8)),
        {{ reporting_date }} between {{ class_start }} and {{ class_end }}
    )
{%- endmacro %}
