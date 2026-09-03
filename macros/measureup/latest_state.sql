{#
    Groups by source_id (raw Django pk), not by a business key — the same
    deliberate simplification made for course_licensing (see
    macros/course_licensing/latest_state.sql for that rationale). Verified
    against measureup_plugin/models.py: ExamTest enforces a DB-level
    UniqueConstraint on (product_id, name, exam_last_updated), not on
    source_id — a republished test version changes exam_last_updated and
    therefore lands as a brand-new Django row with its own source_id.
    Grouping by anything other than source_id would silently collapse
    distinct test versions that must remain distinguishable.
#}
{% macro measureup_latest_state_order() %}
    {#
        Deterministic "most recent event" tuple, same shape as
        course_licensing_latest_state_order(). time_last_dumped must stay
        the PRIMARY field here even more strictly than for course_licensing:
        verified in aspects_plugin/sinks/measure_up/serializers.py that
        ExamDomain, ExamSubDomain and Question each declare a
        model_source_updated_at field sourced from a relation path that
        always terminates at the root ExamTest's exam_last_updated (one
        hop for ExamDomain, two for ExamSubDomain, three for Question) —
        every sibling row under the same ExamTest reports the identical
        source_updated_at regardless of its own real change time, so
        source_updated_at alone cannot discriminate between them.
    #}
    parseDateTime64BestEffort(time_last_dumped, 6),
    parseDateTime64BestEffort(source_updated_at, 6),
    (is_deleted = 'True'),
    sink_event_id
{% endmacro %}


{% macro measureup_argmax_state(value_columns) %}
    {#
        Incrementally track, per source_id, the full row belonging to the
        most recent event — per measureup_latest_state_order(). Meant to
        back a materialized_view using the AggregatingMergeTree engine.
    #}
    argMaxState(
        tuple({{ value_columns | join(", ") }}),
        tuple({{ measureup_latest_state_order() }})
    ) as latest_state
{% endmacro %}


{% macro measureup_argmax_merge(value_columns) %}
    {#
        Unpack the state produced by measureup_argmax_state() back into
        individual named columns, in the same order they were packed.
    #}
    {%- for column in value_columns %}
        (
            argMaxMerge(latest_state)
        ).{{ loop.index }} as {{ column }}{{ "," if not loop.last }}
    {% endfor -%}
{% endmacro %}
