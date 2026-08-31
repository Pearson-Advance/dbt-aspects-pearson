{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_licenses",
            "course_licensing_current",
        ],
    )
}}

select *
from {{ ref("int_course_licensing_license_order_latest_state") }}
where is_deleted != 'True'
