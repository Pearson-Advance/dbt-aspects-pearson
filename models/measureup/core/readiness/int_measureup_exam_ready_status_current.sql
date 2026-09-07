{{
    config(
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_readiness",
            "measureup_current",
        ],
    )
}}

select *
from {{ ref("int_measureup_exam_ready_status_latest_state") }}
where is_deleted != 'True'
