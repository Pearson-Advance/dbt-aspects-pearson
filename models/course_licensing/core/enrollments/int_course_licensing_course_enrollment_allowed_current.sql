{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_enrollments",
            "course_licensing_current",
        ],
    )
}}

select *
from {{ ref("int_course_licensing_course_enrollment_allowed_latest_state") }}
where is_deleted != 'True'
