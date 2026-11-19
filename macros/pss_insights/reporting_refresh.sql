{% macro pss_reporting_upstream_mvs(role) %}
    {% set defaults = {
        "catalog": ["dim_course_blocks_mv"],
        "delivery": [
            "dim_course_blocks_mv",
            "rpt_pss_activity_attempts_mv",
            "rpt_pss_activity_scores_current_mv",
        ],
        "learner": [
            "dim_course_blocks_mv",
            "rpt_pss_activity_attempts_mv",
            "rpt_pss_activity_scores_current_mv",
            "rpt_pss_learner_readiness_current_mv",
        ],
        "class": ["rpt_pss_practice_subdomain_results_mv"],
    } %}
    {% set names = var("pss_reporting_refresh_dependencies", {}).get(
        role, defaults[role]
    ) %}
    {% set relations = [] %}
    {% for name in names %}
        {% do relations.append(name if "." in name else target.schema ~ "." ~ name) %}
    {% endfor %}
    {{ return(relations) }}
{% endmacro %}
