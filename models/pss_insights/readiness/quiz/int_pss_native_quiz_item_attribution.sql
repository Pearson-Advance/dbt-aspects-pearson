{{ config(schema="pss_restricted", tags=["pss_readiness", "pss_quiz_weakness"]) }}

with
    contexts as (
        select distinct
            ccx_id,
            actor_id,
            source_result_id,
            attempt_key,
            attempted_at,
            subsection_block_id,
            problem_block_id,
            snapshot_id,
            notEmpty(trim(evidence_ref)) as sitting_verified,
            notEmpty(trim(mapping_ref)) as mapping_verified,
            -- Remove a redundant parent when its child in the same taxonomy/domain
            -- is also present. Equal allocation applies to the remaining distinct
            -- objectives.
            arraySort(
                arrayFilter(
                    x -> x .3 != 0
                    or not arrayExists(
                        y -> y .1 = x .1 and y .2 = x .2 and y .3 != 0, objectives
                    ),
                    arrayDistinct(if(mapping_verified, objectives, []))
                )
            ) as claimed_keys
        from {{ ref("int_pss_native_quiz_context") }}
    ),
    counted_claims as (
        select *, count() over (partition by ccx_id, source_result_id) as claims
        from contexts
    ),
    checked as (
        select
            *,
            max(claims) over (partition by ccx_id, actor_id, attempt_key) as max_claims,
            uniqExact(tuple(attempted_at, subsection_block_id, snapshot_id)) over (
                partition by ccx_id, actor_id, attempt_key
            ) as sitting_contexts,
            uniqExact(source_result_id) over (
                partition by ccx_id, actor_id, attempt_key, problem_block_id
            ) as result_count
        from counted_claims
        qualify
            row_number() over (
                partition by ccx_id, source_result_id order by attempt_key, actor_id
            )
            = 1
    ),
    evidence as (
        select
            c.institution_id as institution_id,
            c.master_course as master_course,
            c.ccx_id as ccx_id,
            n.actor_id as actor_id,
            coalesce(
                nullIf(n.source_result_id, ''), k.source_result_id
            ) as source_result_id,
            coalesce(
                nullIf(k.problem_block_id, ''), n.problem_block_id
            ) as problem_block_id,
            n.earned_points as earned_points,
            n.possible_points as possible_points,
            n.statement_at as statement_at,
            ifNull(n.statement_at, toDateTime64(0, 6, 'UTC')) as response_cutoff,
            k.subsection_block_id as subsection_block_id,
            k.attempt_key as claimed_attempt_key,
            if(notEmpty(k.attempt_key), k.attempted_at, null) as attempted_at,
            k.snapshot_id as snapshot_id,
            k.claimed_keys as claimed_keys,
            k.mapping_verified as mapping_verified,
            k.result_count as result_count,
            k.actor_id as claimed_actor,
            multiIf(
                empty(ifNull(n.source_result_id, '')),
                'source_result_unavailable',
                n.actor_id is null
                or n.linkage_status
                in ('conflicting_statement', 'registered_actor_unresolved'),
                'source_identity_unverified',
                empty(k.source_result_id),
                'whole_sitting_key_unavailable',
                k.max_claims != 1 or k.sitting_contexts != 1,
                'conflicting_sitting_context',
                k.sitting_verified = 0
                or not startsWith(k.attempt_key, 'native:')
                or length(k.attempt_key) <= 7
                or n.actor_id != k.actor_id
                or not startsWith(k.problem_block_id, 'block-v1:')
                or position(k.problem_block_id, '+type@problem+block@') = 0
                or {{ pss_course_key_from_block_key("k.problem_block_id") }}
                != c.master_course
                or (
                    ifNull(n.source_problem_usage_key, '') != k.problem_block_id
                    and ifNull(n.source_problem_usage_key, '')
                    != {{ pss_ccx_block_key("k.problem_block_id", "c.ccx_number") }}
                ),
                'item_link_unverified',
                empty(ifNull(h.snapshot_id, ''))
                or h.activity_type != 'Quiz'
                or k.attempted_at < h.valid_from
                or (h.valid_until is not null and k.attempted_at >= h.valid_until)
                or (n.statement_at is not null and n.statement_at < k.attempted_at),
                'publication_unverified',
                'linked'
            ) as sitting_status
        from {{ ref("int_pss_native_item_evidence") }} as n
        full outer join
            checked as k
            on k.ccx_id = n.ccx_id
            and k.source_result_id = n.source_result_id
        inner join
            {{ ref("int_pss_class_scope") }} as c
            on c.ccx_id = coalesce(nullIf(n.ccx_id, ''), k.ccx_id)
        left join
            {{ ref("int_pss_activity_type_history") }} as h
            on h.ccx_id = n.ccx_id
            and h.master_course = c.master_course
            and h.subsection_block_id = k.subsection_block_id
            and h.snapshot_id = k.snapshot_id
        where n.current_activity_type = 'Quiz' or notEmpty(k.source_result_id)
    ),
    captures as (
        select master_course, observed_at, capture_id, capture_status
        from {{ ref("int_pss_course_tag_captures") }}
        where problem_block_id = ''
    )
select
    e.institution_id as institution_id,
    e.master_course as master_course,
    e.ccx_id as ccx_id,
    e.actor_id as actor_id,
    e.source_result_id as source_result_id,
    e.problem_block_id as problem_block_id,
    e.earned_points as earned_points,
    e.possible_points as possible_points,
    e.subsection_block_id as subsection_block_id,
    e.claimed_attempt_key as claimed_attempt_key,
    e.attempted_at as attempted_at,
    e.statement_at as statement_at,
    e.snapshot_id as snapshot_id,
    if(e.statement_at is not null, d.capture_id, null) as tag_capture_id,
    if(
        e.statement_at is not null and notEmpty(d.capture_status), d.observed_at, null
    ) as tags_observed_at,
    multiIf(
        e.statement_at is null,
        'response_time_unavailable',
        empty(d.capture_status),
        'no_capture',
        d.capture_status != 'observed',
        d.capture_status,
        empty(m.problem_block_id),
        'problem_absent',
        m.mapping_status != 'observed',
        m.mapping_status,
        m.subsection_block_id is null,
        'placement_unavailable',
        'observed'
    ) as tag_evidence_status,
    e.mapping_verified as has_mapping_claim,
    e.claimed_keys as claimed_objective_keys,
    m.objective_keys as objective_keys,
    m.labels as labels,
    multiIf(
        e.sitting_status != 'linked',
        e.sitting_status,
        e.statement_at is not null
        and (
            h.activity_type != 'Quiz'
            or (h.valid_until is not null and e.statement_at >= h.valid_until)
        ),
        'publication_unverified',
        d.capture_status = 'observed'
        and m.subsection_block_id is not null
        and m.subsection_block_id != e.subsection_block_id,
        'item_link_unverified',
        'linked'
    ) as link_status,
    countIf(link_status != 'linked' and notEmpty(e.claimed_attempt_key)) over (
        partition by e.ccx_id, e.claimed_actor, e.claimed_attempt_key
    ) as unlinked_claims,
    multiIf(
        link_status != 'linked',
        link_status,
        e.result_count != 1,
        'item_selection_unverified',
        tag_evidence_status != 'observed',
        'mapping_at_attempt_unverified',
        e.mapping_verified and e.claimed_keys != m.objective_keys,
        'mapping_changed_or_unavailable',
        empty(m.objective_keys),
        'unmapped',
        'mapped'
    ) as mapping_status
from evidence as e
-- Select the course export first so removing a problem ends its old assignments.
asof left join
    captures as d
    on e.master_course = d.master_course
    and e.response_cutoff >= d.observed_at
left join
    {{ ref("int_pss_quiz_problem_objectives") }} as m
    on m.master_course = e.master_course
    and m.observed_at = d.observed_at
    and m.capture_id = d.capture_id
    and m.problem_block_id = e.problem_block_id
asof left join
    {{ ref("int_pss_activity_type_history") }} as h
    on e.ccx_id = h.ccx_id
    and e.master_course = h.master_course
    and e.subsection_block_id = h.subsection_block_id
    and e.response_cutoff >= h.valid_from
    settings join_use_nulls = 0
