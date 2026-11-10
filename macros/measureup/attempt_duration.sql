{% macro measureup_attempt_duration_seconds(duration_expression) %}
    case
        when {{ duration_expression }} is null
        then cast(null as Nullable(Decimal64(6)))

        when
            not match(
                ifNull({{ duration_expression }}, ''),
                '^[0-9]+:[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?$'
            )
        then cast(null as Nullable(Decimal64(6)))

        else
            toDecimal64OrNull(
                splitByChar(':', ifNull({{ duration_expression }}, ''))[1], 6
            )
            * 3600
            + toDecimal64OrNull(
                splitByChar(':', ifNull({{ duration_expression }}, ''))[2], 6
            )
            * 60
            + toDecimal64OrNull(
                splitByChar(':', ifNull({{ duration_expression }}, ''))[3], 6
            )
    end
{% endmacro %}
