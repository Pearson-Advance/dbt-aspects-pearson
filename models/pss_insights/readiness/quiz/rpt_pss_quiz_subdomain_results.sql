{{
    config(
        tags=["pss_readiness", "pss_quiz_weakness"],
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(ifNull(institution_id, ''), ifNull(ccx_id, ''), ifNull(actor_id, ''), provider, ifNull(attempt_key, ''))",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [
                target.schema ~ ".rpt_pss_activity_attempts_mv",
                target.schema ~ ".dim_course_blocks_mv",
                target.schema ~ "_pss_restricted.int_pss_native_item_evidence_mv",
                target.schema
                ~ "_pss_restricted.int_pss_measureup_quiz_item_evidence_mv",
            ],
            "depends_on_validation": true,
        },
    )
}}

with
    objectives as (
        select
            institution_id,
            ccx_id,
            master_course,
            actor_id,
            subsection_block_id,
            provider,
            attempt_key,
            attempted_at,
            assessment_version_id,
            version_status,
            taxonomy_id,
            domain_id,
            subdomain_id,
            min(domain_name) as domain_name,
            min(subdomain_name) as subdomain_name,
            scoring_rule,
            uniqExact(item_key) as observed_items,
            countIf(item_status = 'scored') as scored_items,
            arraySort(groupUniqArray(item_status)) as item_statuses,
            sumOrNull(numerator) as observed_numerator,
            sumOrNull(denominator) as observed_denominator,
            max(quarantined_items) as quarantined_items,
            countIf(
                item_status in (
                    'item_selection_unverified',
                    'mapping_changed_or_unavailable',
                    'mapping_at_attempt_unverified',
                    'objective_unverified',
                    'no_selected_items'
                )
                and domain_id is null
            ) as unknown_assignments
        from {{ ref("int_pss_quiz_objective_items") }}
        group by
            institution_id,
            ccx_id,
            master_course,
            actor_id,
            subsection_block_id,
            provider,
            attempt_key,
            attempted_at,
            assessment_version_id,
            version_status,
            taxonomy_id,
            domain_id,
            subdomain_id,
            scoring_rule
    )
select
    *,
    max(quarantined_items) over attempt
    + sum(unknown_assignments) over attempt as attempt_mapping_gaps,
    multiIf(
        item_statuses != ['scored'],
        arrayStringConcat(item_statuses, ','),
        attempt_mapping_gaps > 0,
        'incomplete_item_mapping',
        subdomain_id is null,
        'domain_only',
        'scored'
    ) as coverage_status,
    if(
        coverage_status in ('scored', 'domain_only'),
        observed_numerator / nullIf(observed_denominator, 0),
        null
    ) as score,
    if(subdomain_id is not null, score < 0.70, null) as is_weak
from objectives
window attempt as (partition by institution_id, ccx_id, actor_id, provider, attempt_key)
