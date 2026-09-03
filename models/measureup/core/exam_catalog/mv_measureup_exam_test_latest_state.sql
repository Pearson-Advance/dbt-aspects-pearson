{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("AggregatingMergeTree()"),
        primary_key="(source_id)",
        order_by="(source_id)",
        tags=[
            "measureup",
            "measureup_exam_catalog",
            "measureup_latest_state",
        ],
    )
}}

{% set value_columns = measureup_exam_test_columns() %}

select source_id, {{ measureup_argmax_state(value_columns) }}
from {{ source("measureup_event_sink", "measureup_exam_test") }}
group by source_id
