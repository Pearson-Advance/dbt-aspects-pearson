{% macro measureup_attempt_duration_seconds(duration_expression) %}
    case
        when {{ duration_expression }} is null
        then null
        when
            not match(
                {{ duration_expression }},
                '^[0-9]+:[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,6})?$'
            )
        then null
        else
            toDecimal64OrNull(splitByChar(':', {{ duration_expression }})[1], 6) * 3600
            + toDecimal64OrNull(splitByChar(':', {{ duration_expression }})[2], 6) * 60
            + toDecimal64OrNull(splitByChar(':', {{ duration_expression }})[3], 6)
    end
{% endmacro %}
