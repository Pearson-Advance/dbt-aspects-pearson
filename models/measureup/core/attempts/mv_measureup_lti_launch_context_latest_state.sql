{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("AggregatingMergeTree()"),
        primary_key="(source_id)",
        order_by="(source_id)",
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_lti_context",
            "measureup_latest_state",
        ],
    )
}}

{% set value_columns = measureup_lti_launch_context_columns() %}

select source_id, {{ measureup_argmax_state(value_columns) }}
from {{ source("measureup_event_sink", "measureup_lti_launch_context") }}
group by source_id
