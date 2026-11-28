{# dbt's unit-test CTE injector counts each leading space toward sqlparse's token limit. #}
{% set query = pss_learner_readiness(
    ref("fixture_authorized_classes"),
    "toDateTime64('2026-11-02 10:00:00', 6, 'UTC')",
    "toDateTime64('2026-11-02 11:00:00', 6, 'UTC')",
) %}
{{ modules.re.sub("(?m)^[ \t]+", "", query) }}
