{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_bulk_registration",
            "course_licensing_current",
        ],
    )
}}

select *
from {{ ref("int_course_licensing_bulk_user_register_task_latest_state") }}
where is_deleted != 'True'
