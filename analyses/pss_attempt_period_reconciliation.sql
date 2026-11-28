-- Operator reconciliation over current class scope. Supply both ISO UTC bounds
-- as dbt vars when compiling. No report access is granted by this analysis.
{% set bounds = [] %}
{% for name in ["pss_period_start_utc", "pss_period_end_utc"] %}
    {% set value = var(name, "") %}
    {% if value and (
        value is not string
        or not modules.re.fullmatch(
            "[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?Z?",
            value,
        )
    ) %}
        {{
            exceptions.raise_compiler_error(
                name ~ " requires an ISO UTC date and time."
            )
        }}
    {% endif %}
    {% do bounds.append("parseDateTime64BestEffortOrNull('" ~ value ~ "', 6, 'UTC')") %}
{% endfor %}
{{ pss_activity_attempt_counts(bounds[0], bounds[1]) }}
