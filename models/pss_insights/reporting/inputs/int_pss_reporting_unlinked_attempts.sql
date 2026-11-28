{{
    config(
        schema="pss_restricted",
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(ifNull(institution_id, ''), ifNull(ccx_id, ''), ifNull(actor_id, ''), exam_attempt_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [target.schema ~ ".dim_course_blocks_mv"],
            "depends_on_validation": true,
        },
    )
}}

-- A persisted sitting by a registered learner can contradict an otherwise valid
-- completeness claim. A launch alone is never participation evidence.
select
    a.exam_attempt_id as exam_attempt_id,
    a.institution_id as institution_id,
    a.ccx_id as ccx_id,
    a.actor_id as actor_id,
    nullIf(a.subsection_block_id, '') as subsection_block_id,
    a.type_at_start as activity_type,
    a.attempted_at as attempted_at
from {{ ref("int_pss_measureup_attempt_attribution") }} as a
where
    a.resolved_link_status != 'linked'
    and (a.ccx_id, a.actor_id)
    in (select ccx_id, actor_id from {{ ref("int_pss_reporting_roster") }})
