{% test measureup_valid_json(model, column_name) %}
    select {{ adapter.quote(column_name) }} as invalid_json
    from {{ model }}
    where
        {{ adapter.quote(column_name) }} is not null
        and {{ adapter.quote(column_name) }} != ''
        and not isValidJSON({{ adapter.quote(column_name) }})
{% endtest %}
