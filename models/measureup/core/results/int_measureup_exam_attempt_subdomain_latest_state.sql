{{
    config(
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_results",
            "measureup_latest_state",
        ],
    )
}}

{% set value_columns = measureup_exam_attempt_subdomain_columns() %}

select source_id, {{ measureup_argmax_merge(value_columns) }}
from {{ ref("mv_measureup_exam_attempt_subdomain_latest_state") }}
group by source_id
