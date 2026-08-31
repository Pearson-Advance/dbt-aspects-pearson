{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_enrollments",
            "course_licensing_latest_state",
        ],
    )
}}

{% set value_columns = course_licensing_course_enrollment_allowed_columns() %}

select source_id, {{ course_licensing_argmax_merge(value_columns) }}
from {{ ref("mv_course_licensing_course_enrollment_allowed_latest_state") }}
group by source_id
