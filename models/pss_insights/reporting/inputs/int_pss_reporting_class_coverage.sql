-- Positive evidence for all-time class coverage, including identity resolution.
-- An absent row leaves the affected metrics unavailable.
{% if var("pss_class_coverage_model", none) %}
    select ccx_id, final_attempts_complete, navigation_complete
    from {{ ref(var("pss_class_coverage_model")) }}
{% else %}
    select
        cast('' as String) as ccx_id,
        toUInt8(0) as final_attempts_complete,
        toUInt8(0) as navigation_complete
    where false
{% endif %}
