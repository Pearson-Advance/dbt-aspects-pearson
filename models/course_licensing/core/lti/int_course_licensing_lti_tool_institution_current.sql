{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_lti",
            "course_licensing_current",
        ],
    )
}}

select *
from {{ ref("int_course_licensing_lti_tool_institution_latest_state") }}
where is_deleted != 'True'
