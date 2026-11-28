{{ config(tags=["pss_readiness"]) }}

-- Compute the whole attempt across all objective buckets, including the
-- unattributed bucket. A known unresolved denominator invalidates the sitting.
select
    institution_id,
    master_course,
    ccx_id,
    actor_id,
    subsection_block_id,
    attempt_key,
    exam_attempt_id,
    exam_test_id,
    attempted_at,
    version_status,
    multiIf(
        sum(empty_branches) > 0,
        'missing_item_evidence',
        sum(invalid_items) > 0,
        'unresolved_items',
        sum(disputed_items) > 0,
        'item_policy_required',
        sum(observed_total_items) = 0,
        'zero_eligible_items',
        'counted'
    ) as coverage_status,
    if(coverage_status = 'counted', sum(observed_correct_items), null) as correct_items,
    if(coverage_status = 'counted', sum(observed_total_items), null) as total_items
from {{ ref("rpt_pss_practice_subdomain_results") }}
group by
    institution_id,
    master_course,
    ccx_id,
    actor_id,
    subsection_block_id,
    attempt_key,
    exam_attempt_id,
    exam_test_id,
    attempted_at,
    version_status
