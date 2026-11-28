{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(institution_id, ccx_id, actor_id, attempt_key)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [target.schema ~ ".rpt_pss_activity_attempts_mv"],
            "depends_on_validation": true,
        },
        tags=["pss_readiness"],
    )
}}

with
    buckets as (
        select
            institution_id,
            master_course,
            ccx_id,
            actor_id,
            subsection_block_id,
            'measureup' as provider,
            attempt_key,
            exam_attempt_id,
            exam_test_id,
            version_status,
            attempted_at,
            domain_id,
            subdomain_id,
            min(objective_domain_name) as domain_name,
            min(objective_subdomain_name) as subdomain_name,
            item_rule,
            countIf(item_outcome = 'correct') as observed_correct_items,
            countIf(item_outcome in ('correct', 'incorrect')) as observed_total_items,
            countIf(item_outcome = 'excluded') as excluded_items,
            countIf(item_outcome = 'missing_item_evidence') as empty_branches,
            countIf(
                item_outcome in ('unresolved_item', 'invalid_score')
            ) as invalid_items,
            countIf(item_outcome = 'item_policy_required') as disputed_items,
            multiIf(
                empty_branches > 0,
                'missing_item_evidence',
                invalid_items > 0,
                'unresolved_items',
                disputed_items > 0,
                'item_policy_required',
                'counted'
            ) as item_coverage_status
        from {{ ref("int_pss_practice_item_counts") }}
        group by
            institution_id,
            master_course,
            ccx_id,
            actor_id,
            subsection_block_id,
            attempt_key,
            exam_attempt_id,
            exam_test_id,
            version_status,
            attempted_at,
            domain_id,
            subdomain_id,
            item_rule

    )
select
    *,
    countIf(
        subdomain_id is null
        and (item_coverage_status != 'counted' or observed_total_items > 0)
    ) over (partition by institution_id, ccx_id, actor_id, attempt_key)
    as unassigned_buckets,
    if(
        subdomain_id is not null
        and item_coverage_status = 'counted'
        and unassigned_buckets > 0,
        'incomplete_item_mapping',
        item_coverage_status
    ) as coverage_status,
    if(coverage_status = 'counted', observed_correct_items, null) as correct_items,
    if(coverage_status = 'counted', observed_total_items, null) as total_items,
    if(
        subdomain_id is not null, correct_items / nullIf(total_items, 0), null
    ) as subdomain_score,
    subdomain_score < 0.70 as is_weak,
    if(subdomain_id is null, 'objective_unavailable', 'verified') as mapping_status
from buckets
