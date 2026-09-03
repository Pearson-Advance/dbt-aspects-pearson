{{
    config(
        tags=[
            "measureup",
            "measureup_exam_catalog",
            "measureup_current",
        ],
    )
}}

select *
from {{ ref("int_measureup_exam_domain_latest_state") }}
where is_deleted != 'True'
