{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_classes",
            "course_licensing_current",
        ],
    )
}}

select *
from {{ ref("int_course_licensing_institution_ccx_latest_state") }}
where is_deleted != 'True'
