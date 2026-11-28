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
                    arrayDistinct(objectives)
                )
            ) as objective_keys
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
    maps as (
        select
            master_course,
            subsection_block_id,
            problem_block_id,
            arraySort(
                groupUniqArray(
                    tuple(
                        toUInt64(taxonomy_id),
                        toUInt64(ifNull(domain_tag_id, 0)),
                        toUInt64(ifNull(subdomain_tag_id, 0))
                    )
                )
            ) as current_keys,
            groupUniqArray(
                tuple(
                    toUInt64(taxonomy_id),
                    toUInt64(ifNull(domain_tag_id, 0)),
                    toUInt64(ifNull(subdomain_tag_id, 0)),
                    ifNull(domain_name, ''),
                    ifNull(subdomain_name, '')
                )
            ) as labels
        from {{ ref("int_pss_quiz_item_subdomain_map") }}
        where in_latest_publish and graded
        group by master_course, subsection_block_id, problem_block_id
    )
select
    c.institution_id as institution_id,
    c.master_course as master_course,
    c.ccx_id as ccx_id,
    n.actor_id as actor_id,
    coalesce(nullIf(n.source_result_id, ''), k.source_result_id) as source_result_id,
    n.problem_block_id as problem_block_id,
    n.earned_points as earned_points,
    n.possible_points as possible_points,
    k.subsection_block_id as subsection_block_id,
    k.attempt_key as claimed_attempt_key,
    if(notEmpty(k.attempt_key), k.attempted_at, null) as attempted_at,
    k.snapshot_id as snapshot_id,
    k.objective_keys as objective_keys,
    m.labels as labels,
    multiIf(
        empty(ifNull(n.source_result_id, '')),
        'source_result_unavailable',
        n.actor_id is null or n.linkage_status != 'whole_sitting_key_unavailable',
        'source_identity_unverified',
        empty(k.source_result_id),
        'whole_sitting_key_unavailable',
        k.max_claims != 1 or k.sitting_contexts != 1,
        'conflicting_sitting_context',
        k.sitting_verified = 0
        or not startsWith(k.attempt_key, 'native:')
        or length(k.attempt_key) <= 7
        or n.actor_id != k.actor_id
        or n.problem_block_id is null
        or n.problem_block_id != k.problem_block_id
        or n.current_subsection_block_id != k.subsection_block_id,
        'item_link_unverified',
        empty(ifNull(h.snapshot_id, ''))
        or h.activity_type != 'Quiz'
        or k.attempted_at < h.valid_from
        or (h.valid_until is not null and k.attempted_at >= h.valid_until)
        or (n.statement_at is not null and n.statement_at < k.attempted_at),
        'publication_unverified',
        'linked'
    ) as link_status,
    countIf(link_status != 'linked' and notEmpty(k.attempt_key)) over (
        partition by k.ccx_id, k.actor_id, k.attempt_key
    ) as unlinked_claims,
    multiIf(
        link_status != 'linked',
        link_status,
        k.result_count != 1,
        'item_selection_unverified',
        not k.mapping_verified,
        'mapping_at_attempt_unverified',
        k.objective_keys != m.current_keys,
        'mapping_changed_or_unavailable',
        empty(k.objective_keys),
        'unmapped',
        arrayExists(x -> x .1 = 0 or x .2 = 0, k.objective_keys),
        'mapping_at_attempt_unverified',
        'mapped'
    ) as mapping_status
from {{ ref("int_pss_native_item_evidence") }} as n
full outer join
    checked as k on k.ccx_id = n.ccx_id and k.source_result_id = n.source_result_id
inner join
    {{ ref("int_pss_class_scope") }} as c
    on c.ccx_id = coalesce(nullIf(n.ccx_id, ''), k.ccx_id)
left join
    {{ ref("int_pss_activity_type_history") }} as h
    on h.ccx_id = n.ccx_id
    and h.master_course = c.master_course
    and h.subsection_block_id = k.subsection_block_id
    and h.snapshot_id = k.snapshot_id
left join
    maps as m
    on m.master_course = c.master_course
    and m.subsection_block_id = k.subsection_block_id
    and m.problem_block_id = k.problem_block_id
where
    n.current_activity_type = 'Quiz' or notEmpty(k.source_result_id)
    settings join_use_nulls = 0
