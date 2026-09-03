{#
    Canonical column contract for each MeasureUp entity's raw sink row
    (excluding source_id, which every mv_/latest_state model selects
    separately). Mirrors macros/course_licensing/entity_columns.sql — see
    that file's header for the shared rationale. Each entity's list is
    defined exactly once here and shared by both its mv_ latest-state model
    (measureup_argmax_state) and its finalized latest-state view
    (measureup_argmax_merge), so the two can never drift out of sync.
    Column order matters: argMaxMerge unpacks the packed tuple positionally.

    Covers the full test catalog hierarchy: exam_test (root), exam_domain,
    exam_subdomain, question (leaf).
#}
{% macro measureup_exam_test_columns() %}
    {{
        return(
            [
                "external_id",
                "name",
                "default_question_count",
                "product_id",
                "exam_time",
                "cut_score",
                "reference",
                "exam_mode",
                "exam_language",
                "exam_last_updated",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}


{% macro measureup_exam_domain_columns() %}
    {{
        return(
            [
                "external_id",
                "exam_test_id",
                "name",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}


{% macro measureup_exam_subdomain_columns() %}
    {{
        return(
            [
                "external_id",
                "exam_domain_id",
                "name",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}


{% macro measureup_question_columns() %}
    {{
        return(
            [
                "external_id",
                "exam_subdomain_id",
                "name",
                "question_type",
                "content",
                "is_demo",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}
