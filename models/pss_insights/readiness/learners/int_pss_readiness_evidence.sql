{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(institution_id, ccx_id, actor_id, subsection_block_id, evidence_kind, source_key)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [
                target.schema ~ ".rpt_pss_activity_attempts_mv",
                target.schema ~ ".rpt_pss_activity_scores_current_mv",
                target.schema ~ ".rpt_pss_practice_subdomain_results_mv",
            ],
            "depends_on_validation": true,
        },
        tags=["pss_readiness"],
    )
}}
-- Preserve the distinct sitting before any classroom, item or taxonomy aggregation.
select
    a.institution_id as institution_id,
    a.ccx_id as ccx_id,
    a.master_course as master_course,
    a.actor_id as actor_id,
    a.subsection_block_id as subsection_block_id,
    a.attempt_key as source_key,
    'sitting' as evidence_kind,
    a.activity_type as activity_type,
    a.attempted_at as started_at,
    a.attempt_finished_at as observed_at,
    a.is_finished as finished,
    if(
        a.activity_type = 'Practice Exam', a.is_eligible_practice, a.is_participation
    ) as eligible,
    a.score_pct as score_pct,
    cast(null as Nullable(String)) as selection_rule,
    toUInt64(0) as native_grade_count,
    cast([], 'Array(String)') as selected_provider_attempt_keys,
    if(empty(ifNull(p.attempt_key, '')), null, p.correct_items) as correct_items,
    if(empty(ifNull(p.attempt_key, '')), null, p.total_items) as total_items
from {{ ref("rpt_pss_activity_attempts") }} as a
left join
    {{ ref("int_pss_practice_attempt_totals") }} as p
    on a.ccx_id = p.ccx_id
    and a.actor_id = p.actor_id
    and a.attempt_key = p.attempt_key
where a.activity_type is null or a.activity_type in ('Quiz', 'Lab', 'Practice Exam')
union all
select
    assumeNotNull(institution_id) as institution_id,
    ccx_id,
    master_course,
    actor_id,
    subsection_block_id,
    subsection_block_id as source_key,
    'current_score' as evidence_kind,
    activity_type,
    null as started_at,
    score_observed_at as observed_at,
    false as finished,
    null as eligible,
    score_pct,
    selection_rule,
    native_grade_count,
    arrayMap(
        x -> x .2, arrayFilter(x -> x .1 = 'measureup', selected_source_ids)
    ) as selected_provider_attempt_keys,
    null as correct_items,
    null as total_items
from {{ ref("rpt_pss_activity_scores_current") }}
where activity_type in ('Quiz', 'Lab')
