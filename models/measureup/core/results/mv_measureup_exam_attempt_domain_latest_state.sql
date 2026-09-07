{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("AggregatingMergeTree()"),
        primary_key="(source_id)",
        order_by="(source_id)",
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_results",
            "measureup_latest_state",
        ],
    )
}}

{% set value_columns = measureup_exam_attempt_domain_columns() %}

select source_id, {{ measureup_argmax_state(value_columns) }}
from {{ source("measureup_event_sink", "measureup_exam_attempt_domain") }}
group by source_id
