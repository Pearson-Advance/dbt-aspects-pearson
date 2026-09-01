{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("AggregatingMergeTree()"),
        primary_key="(source_id)",
        order_by="(source_id)",
        tags=[
            "course_licensing",
            "course_licensing_instructor_assignments",
            "course_licensing_courses_instructors",
            "course_licensing_latest_state",
        ],
    )
}}

{% set value_columns = course_licensing_instructor_columns() %}
select source_id, {{ course_licensing_argmax_state(value_columns) }}
from {{ source("course_licensing_event_sink", "course_licensing_instructor") }}
group by source_id
