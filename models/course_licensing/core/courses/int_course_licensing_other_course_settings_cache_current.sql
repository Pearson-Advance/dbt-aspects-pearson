{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_courses",
            "course_licensing_courses_instructors",
            "course_licensing_current",
        ],
    )
}}

select *
from {{ ref("int_course_licensing_other_course_settings_cache_latest_state") }}
where is_deleted != 'True'
