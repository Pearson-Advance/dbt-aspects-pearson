{{ config(schema="pss_restricted", tags=["pss_scores"]) }}

-- Keep every surviving provider attempt and its reason if it cannot enter the
-- learner fact. Neither names, email addresses nor restricted JSON are needed.
with
    -- An observed change to a version identity or pass mark under the same PK
    -- prevents attaching its current threshold to a historical attempt.
    version_conflicts as (
        select source_id, toUInt8(1) as conflict
        from {{ source("measureup_event_sink", "measureup_exam_test") }}
        where is_deleted != 'True'
        group by source_id
        having
            uniqExact(
                tuple(
                    product_id,
                    name,
                    parseDateTime64BestEffortOrNull(exam_last_updated, 6, 'UTC'),
                    toFloat64OrNull(cut_score)
                )
            )
            > 1
    ),
    launches as (
        select
            l.source_id as launch_id,
            l.course_id as source_course,
            l.user_id as source_user,
            l.block_id as launch_block_id,
            l.product_id as launch_product_id,
            c.ccx_id as ccx_id,
            c.institution_id as institution_id,
            c.master_course as master_course,
            multiIf(
                startsWith(
                    l.block_id,
                    concat('block-v1:', substring(c.master_course, 11), '+type@')
                ),
                l.block_id,
                startsWith(
                    l.block_id,
                    concat('ccx-block-v1:', substring(c.ccx_id, 8), '+type@')
                ),
                concat(
                    'block-v1:',
                    substring(c.master_course, 11),
                    substring(l.block_id, position(l.block_id, '+type@'))
                ),
                ''
            ) as master_block_id
        from {{ ref("int_measureup_lti_launch_context_current") }} as l
        left join {{ ref("int_pss_class_scope") }} as c on l.course_id = c.ccx_id
    )
select
    a.source_id as exam_attempt_id,
    a.exam_test_id as exam_test_id,
    a.lti_launch_event_id as lti_launch_event_id,
    coalesce(l.source_course, '') as source_course,
    l.ccx_id as ccx_id,
    l.master_course as master_course,
    l.institution_id as institution_id,
    r.actor_id as actor_id,
    l.launch_block_id as launch_block_id,
    l.master_block_id as master_block_id,
    b.subsection_block_id as subsection_block_id,
    b.first_parent_observed_at as first_parent_observed_at,
    nullIf(c.activity_type, '') as activity_type,
    nullIf(c.snapshot_id, '') as catalog_snapshot_id,
    c.in_latest_publish as activity_in_latest_publish,
    t.source_id as catalog_test_id,
    t.exam_last_updated as exam_last_updated,
    t.cut_score as raw_cut_score,
    ifNull(history.conflict, 0) as version_conflict,
    a.flow_state as raw_state,
    a.mode as raw_mode,
    a.attempt_initiated_datetime as raw_start,
    a.attempt_finished_datetime as raw_finish,
    a.final_score as raw_score,
    a.is_passed as raw_passed,
    multiIf(
        empty(trim(a.source_id)),
        'missing_attempt_identifier',
        empty(ifNull(l.launch_id, '')),
        'missing_launch',
        empty(ifNull(l.ccx_id, '')),
        'class_out_of_scope',
        empty(ifNull(r.record_key, '')),
        'missing_class_registration',
        r.identity_status != 'resolved',
        r.identity_status,
        ifNull(r.is_registered, 0) != 1,
        'not_registered',
        notEmpty(ifNull(t.product_id, ''))
        and notEmpty(ifNull(l.launch_product_id, ''))
        and t.product_id != l.launch_product_id,
        'conflicting_launch_product',
        empty(ifNull(l.master_block_id, '')),
        'invalid_launch_block_scope',
        empty(ifNull(b.parent_status, '')),
        'unmapped_launch_block',
        b.parent_status != 'linked',
        b.parent_status,
        empty(ifNull(c.activity_type, '')),
        'activity_not_in_catalog',
        'linked'
    ) as link_status
from {{ ref("int_measureup_exam_test_attempt_current") }} as a
left join launches as l on a.lti_launch_event_id = l.launch_id
left join
    {{ ref("int_pss_roster_evidence") }} as r
    on l.ccx_id = r.ccx_id
    and l.source_user = r.lms_user_id
left join
    {{ ref("int_pss_lti_subsection_links") }} as b
    on l.master_course = b.master_course
    and l.master_block_id = b.master_block_id
left join
    {{ ref("int_pss_activity_catalog") }} as c
    on l.ccx_id = c.ccx_id
    and l.master_course = c.master_course
    and b.subsection_block_id = c.subsection_block_id
left join
    {{ ref("int_measureup_exam_test_current") }} as t on a.exam_test_id = t.source_id

left join
    version_conflicts as history on a.exam_test_id = history.source_id
    -- Fix the source join semantics for consumers using join_use_nulls = 1.
    settings join_use_nulls = 0
