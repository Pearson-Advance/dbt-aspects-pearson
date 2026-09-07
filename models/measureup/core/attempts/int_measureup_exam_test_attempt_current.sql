{{
    config(
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_attempts",
            "measureup_current",
        ],
    )
}}

select *
from {{ ref("int_measureup_exam_test_attempt_latest_state") }}
where is_deleted != 'True'
