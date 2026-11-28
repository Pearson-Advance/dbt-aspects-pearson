{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(institution_id, ccx_id, actor_id, subsection_block_id, attempt_key)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [target.schema ~ ".dim_course_blocks_mv"],
        },
        tags=["pss_scores"],
    )
}}

with
    linked as (
        select
            e.*,
            lowerUTF8(trim(ifNull(e.raw_state, ''))) as flow_state,
            lowerUTF8(trim(ifNull(e.raw_mode, ''))) as mode,
            parseDateTime64BestEffortOrNull(
                e.raw_finish, 6, 'UTC'
            ) as attempt_finished_at,
            parseDateTime64BestEffortOrNull(
                e.exam_last_updated, 6, 'UTC'
            ) as version_updated_at,
            toFloat64OrNull(e.raw_score) as observed_score,
            toFloat64OrNull(e.raw_cut_score) as observed_cut_score
        from {{ ref("int_pss_measureup_attempt_attribution") }} as e
        where e.resolved_link_status = 'linked'
    )
select
    assumeNotNull(e.ccx_id) as ccx_id,
    assumeNotNull(e.institution_id) as institution_id,
    assumeNotNull(e.master_course) as master_course,
    assumeNotNull(e.actor_id) as actor_id,
    assumeNotNull(e.subsection_block_id) as subsection_block_id,
    e.type_at_start as activity_type,
    e.classification_status as classification_status,
    e.snapshot_at_start as attempt_catalog_snapshot_id,
    e.activity_type as catalog_activity_type,
    e.catalog_snapshot_id as catalog_snapshot_id,
    e.activity_in_latest_publish as activity_in_latest_publish,
    'measureup' as provider,
    concat('measureup:', e.exam_attempt_id) as attempt_key,
    e.exam_attempt_id as exam_attempt_id,
    e.exam_test_id as exam_test_id,
    e.lti_launch_event_id as lti_launch_event_id,
    e.launch_block_id as launch_block_id,
    e.first_parent_observed_at as first_parent_observed_at,
    e.master_block_id as master_block_id,
    e.raw_mode as raw_mode,
    e.mode as mode,
    e.raw_state as raw_state,
    e.flow_state as flow_state,
    e.attempted_at as attempted_at,
    e.attempt_finished_at as attempt_finished_at,
    e.flow_state = 'finished'
    and e.attempt_finished_at is not null
    and (
        e.attempted_at is null or e.attempt_finished_at >= e.attempted_at
    ) as is_finished,
    n.attempt_number as provider_attempt_number,
    e.raw_score as raw_score,
    nullIf(u.approved_unit, '') as score_unit,
    multiIf(
        empty(trim(ifNull(e.raw_score, ''))),
        'missing_score',
        e.observed_score is null
        or not isFinite(e.observed_score)
        or e.observed_score < 0
        or e.observed_score > 100,
        'invalid_score',
        e.observed_score = 0,
        'observed',
        not ifNull(u.valid_policy, false) or empty(ifNull(u.approved_unit, '')),
        'score_unit_unconfirmed',
        u.approved_unit = 'fraction' and e.observed_score > 1,
        'invalid_score',
        'observed'
    ) as score_status,
    if(
        score_status = 'observed',
        if(u.approved_unit = 'fraction', e.observed_score * 100, e.observed_score),
        null
    ) as score_pct,
    multiIf(
        lower(trim(ifNull(e.raw_passed, ''))) in ('true', '1'),
        toBool(1),
        lower(trim(ifNull(e.raw_passed, ''))) in ('false', '0'),
        toBool(0),
        null
    ) as is_passed,
    nullIf(e.catalog_test_id, '') as catalog_test_id,
    e.version_updated_at as version_updated_at,
    if(
        e.observed_cut_score between 0 and 100, e.observed_cut_score, null
    ) as catalog_cut_score,
    multiIf(
        empty(ifNull(e.catalog_test_id, '')) or e.version_updated_at is null,
        'missing_test_version',
        e.version_conflict,
        'conflicting_version_history',
        e.attempted_at is null,
        'missing_start_time',
        e.version_updated_at > e.attempted_at,
        'version_after_start',
        catalog_cut_score is null,
        'invalid_version_mark',
        ifNull(u.valid_policy, false) = 0 or ifNull(u.version_pairing, 0) != 1,
        'version_pairing_unconfirmed',
        'verified'
    ) as version_status,
    if(version_status = 'verified', catalog_cut_score, null) as cut_score,
    multiIf(
        e.classification_status != 'classified',
        e.classification_status,
        empty(ifNull(r.master_course, '')),
        'eligibility_rule_missing',
        not r.valid_rule,
        'invalid_eligibility_rule',
        e.flow_state = 'finished' and not is_finished,
        'finish_time_unverified',
        'approved'
    ) as eligibility_status,
    if(eligibility_status = 'approved', r.participation, null) as is_participation,
    multiIf(
        e.type_at_start != 'Practice Exam',
        toUInt8(0),
        eligibility_status != 'approved',
        null,
        r.practice
    ) as is_eligible_practice,
    multiIf(
        e.type_at_start != 'Final Exam',
        toUInt8(0),
        eligibility_status != 'approved',
        null,
        r.final
    ) as is_qualifying_final,
    multiIf(
        e.type_at_start = 'Final Exam',
        is_qualifying_final,
        e.type_at_start = 'Practice Exam',
        is_eligible_practice,
        is_participation
    ) as sequence_eligibility,
    multiIf(
        sequence_eligibility is null,
        'eligibility_unavailable',
        sequence_eligibility = 0,
        'ineligible',
        e.attempted_at is null,
        'start_time_unavailable',
        empty(ifNull(e.catalog_test_id, '')),
        'test_version_unavailable',
        empty(e.mode),
        'mode_unavailable',
        'numbered'
    ) as numbering_status,
    if(
        numbering_status = 'numbered',
        row_number() over (
            partition by
                e.ccx_id,
                e.actor_id,
                e.subsection_block_id,
                e.exam_test_id,
                e.mode,
                numbering_status
            order by
                e.attempted_at, toUInt64OrZero(e.exam_attempt_id), e.exam_attempt_id
        ),
        null
    ) as attempt_number
from linked as e
left join
    {{ ref("mart_measureup_attempt_numbering") }} as n
    on n.exam_attempt_id = e.exam_attempt_id
left join
    {{ ref("int_pss_measureup_score_units") }} as u on u.master_course = e.master_course
left join
    {{ ref("int_pss_measureup_attempt_rules") }} as r
    on r.master_course = e.master_course
    and r.activity_type = e.type_at_start
    and r.required_state = e.flow_state
    and r.required_mode = e.mode
