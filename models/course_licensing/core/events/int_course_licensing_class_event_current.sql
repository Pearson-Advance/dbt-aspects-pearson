{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_events",
            "course_licensing_current",
        ],
    )
}}

select *
from {{ ref("int_course_licensing_class_event_latest_state") }}
where is_deleted != 'True'
