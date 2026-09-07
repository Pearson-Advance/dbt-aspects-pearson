{{
    config(
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_results",
            "measureup_current",
        ],
    )
}}

select *
from {{ ref("int_measureup_exam_attempt_subdomain_latest_state") }}
where is_deleted != 'True'
