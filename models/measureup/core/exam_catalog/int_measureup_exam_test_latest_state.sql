{{
    config(
        tags=[
            "measureup",
            "measureup_exam_catalog",
            "measureup_latest_state",
        ],
    )
}}

{% set value_columns = measureup_exam_test_columns() %}

select source_id, {{ measureup_argmax_merge(value_columns) }}
from {{ ref("mv_measureup_exam_test_latest_state") }}
group by source_id
