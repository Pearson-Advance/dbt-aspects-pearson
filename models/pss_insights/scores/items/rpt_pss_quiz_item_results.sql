{{ config(tags=["pss_scores", "pss_items"]) }}

-- There is no answer timestamp or provider-defined precedence between two
-- different Django result PKs for one item in one sitting. Keep the item at
-- its reporting grain and withhold the disputed result until the within-attempt
-- item result selection is decided.
select
    institution_id,
    ccx_id,
    master_course,
    subsection_block_id,
    actor_id,
    'measureup' as provider,
    attempt_key,
    source_exam_attempt_id as exam_attempt_id,
    exam_test_id,
    version_status,
    version_updated_at,
    question_id as provider_item_id,
    concat('measureup:question:', question_id) as item_key,
    attempted_at,
    attempt_finished_at,
    if(source_result_count = 1, source_result_id, null) as source_result_id,
    if(
        source_result_count = 1, evidence.item_catalog_status, 'selection_unavailable'
    ) as item_catalog_status,
    if(
        source_result_count = 1 and evidence.item_catalog_status = 'verified',
        exam_domain_id,
        null
    ) as exam_domain_id,
    if(
        source_result_count = 1 and evidence.item_catalog_status = 'verified',
        exam_subdomain_id,
        null
    ) as exam_subdomain_id,
    if(source_result_count = 1, source_updated_at, null) as source_updated_at,
    if(source_result_count = 1, observed_at, null) as observed_at,
    cast(null as Nullable(DateTime64(6, 'UTC'))) as item_answered_at,
    if(source_result_count = 1, provider_status, null) as item_outcome,
    if(source_result_count = 1, provider_score, null) as provider_item_score,
    cast(null as Nullable(Float64)) as earned_points,
    cast(null as Nullable(Float64)) as possible_points,
    source_result_count,
    multiIf(
        source_result_count > 1,
        'within_attempt_selection_unapproved',
        source_score is null and lowerUTF8(ifNull(provider_status, '')) = 'unanswered',
        'unanswered',
        source_score is null,
        'score_missing',
        provider_score is null,
        'score_invalid',
        'provider_score_observed'
    ) as result_status
from {{ ref("int_pss_measureup_quiz_item_evidence") }} as evidence
where linkage_status = 'linked'
qualify
    row_number() over (
        partition by attempt_key, question_id order by evidence.source_result_id
    )
    = 1
