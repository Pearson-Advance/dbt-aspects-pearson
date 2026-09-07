{{
    config(
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_lti_context",
            "measureup_latest_state",
        ],
    )
}}

{% set value_columns = measureup_lti_launch_context_columns() %}

select source_id, {{ measureup_argmax_merge(value_columns) }}
from {{ ref("mv_measureup_lti_launch_context_latest_state") }}
group by source_id
