{#
    Groups by source_id (raw Django pk), not by a business key — a deliberate
    simplification, confirmed with the team. Every entity's source_id already
    guarantees exactly one current row without needing a verified stable
    business key per entity. Trade-off: a delete-then-recreate sequence for
    the same real-world record produces two independent source_id lifecycles
    instead of being merged into one continuous record.
#}
{% macro course_licensing_latest_state_order() %}
    {#
        Deterministic "most recent event" tuple shared by every course_licensing
        current-state model: newest dump wins, then newest source-side update,
        then a delete tombstone outranks an UPSERT recorded at the same instant,
        then the sink event id breaks any remaining tie.
    #}
    parseDateTime64BestEffort(time_last_dumped, 6),
    parseDateTime64BestEffort(source_updated_at, 6),
    (is_deleted = 'True'),
    sink_event_id
{% endmacro %}


{% macro course_licensing_argmax_state(value_columns) %}
    {#
        Incrementally track, per source_id, the full row belonging to the most
        recent event — per course_licensing_latest_state_order(). Meant to back
        a materialized_view using the AggregatingMergeTree engine.
    #}
    argMaxState(
        tuple({{ value_columns | join(", ") }}),
        tuple({{ course_licensing_latest_state_order() }})
    ) as latest_state
{% endmacro %}


{% macro course_licensing_argmax_merge(value_columns) %}
    {#
        Unpack the state produced by course_licensing_argmax_state() back into
        individual named columns, in the same order they were packed.
    #}
    {%- for column in value_columns %}
        (
            argMaxMerge(latest_state)
        ).{{ loop.index }} as {{ column }}{{ "," if not loop.last }}
    {% endfor -%}
{% endmacro %}
