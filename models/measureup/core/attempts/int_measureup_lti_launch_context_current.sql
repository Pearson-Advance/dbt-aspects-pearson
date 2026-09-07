{{
    config(
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_lti_context",
            "measureup_current",
        ],
    )
}}

select *
from {{ ref("int_measureup_lti_launch_context_latest_state") }}
where is_deleted != 'True'
