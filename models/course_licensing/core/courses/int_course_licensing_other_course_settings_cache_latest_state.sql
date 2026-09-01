{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_courses",
            "course_licensing_courses_instructors",
            "course_licensing_latest_state",
        ],
    )
}}

{% set value_columns = course_licensing_other_course_settings_cache_columns() %}

select source_id, {{ course_licensing_argmax_merge(value_columns) }}
from {{ ref("mv_course_licensing_other_course_settings_cache_latest_state") }}
group by source_id
