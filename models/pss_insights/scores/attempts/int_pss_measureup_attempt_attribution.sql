{{ config(schema="pss_restricted", tags=["pss_scores"]) }}

with
    source_attempts as (
        select *, parseDateTime64BestEffortOrNull(raw_start, 6, 'UTC') as attempted_at
        from {{ ref("int_pss_measureup_attempt_evidence") }}
    )
select
    a.* except (ccx_id, subsection_block_id, master_course, activity_type),
    a.ccx_id as ccx_id,
    a.subsection_block_id as subsection_block_id,
    a.master_course as master_course,
    a.activity_type as activity_type,
    multiIf(
        a.link_status not in ('linked', 'activity_not_in_catalog'),
        'unresolved_attempt',
        a.attempted_at is null,
        'start_time_unavailable',
        a.first_parent_observed_at is null,
        'lti_parent_time_unavailable',
        a.attempted_at < a.first_parent_observed_at,
        'attempt_before_observed_lti_parent',
        not ifNull(d.dump_order_known, false)
        or not has(d.lti_blocks, a.master_block_id),
        'lti_block_unverified_at_start',
        empty(ifNull(h.snapshot_id, ''))
        or (h.valid_until is not null and a.attempted_at >= h.valid_until),
        'activity_type_at_start_unavailable',
        'classified'
    ) as classification_status,
    if(classification_status = 'classified', h.activity_type, null) as type_at_start,
    if(classification_status = 'classified', h.snapshot_id, null) as snapshot_at_start,
    -- Reclassification as ungraded or unmapped can remove the subsection
    -- from today's catalog. Restore only a verified dated class assignment.
    multiIf(
        classification_status in (
            'lti_parent_time_unavailable',
            'attempt_before_observed_lti_parent',
            'lti_block_unverified_at_start'
        ),
        classification_status,
        a.link_status = 'activity_not_in_catalog'
        and classification_status = 'classified',
        'linked',
        a.link_status
    ) as resolved_link_status
from source_attempts as a
asof left join
    {{ ref("int_pss_activity_type_history") }} as h
    on a.ccx_id = h.ccx_id
    and a.subsection_block_id = h.subsection_block_id
    and ifNull(a.attempted_at, toDateTime64(0, 6, 'UTC')) >= h.valid_from

asof left join
    {{ ref("int_pss_lti_dump_membership") }} as d
    on a.master_course = d.master_course
    and ifNull(a.attempted_at, toDateTime64(0, 6, 'UTC')) >= d.observed_at_utc
