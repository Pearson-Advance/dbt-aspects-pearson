{{ config(schema="pss_restricted", tags=["pss_readiness", "pss_quiz_weakness"]) }}

{% set rules = var("pss_quiz_weakness_rules", {}) %}
{% if rules is not mapping %}
    {{
        exceptions.raise_compiler_error(
            "pss_quiz_weakness_rules must map master courses to provider rules"
        )
    }}
{% endif %}
{% for master, providers in rules.items() %}
    {% if master is not string or not master.startswith(
        "course-v1:"
    ) or providers is not mapping %}
        {{
            exceptions.raise_compiler_error(
                "Invalid master or provider map in pss_quiz_weakness_rules"
            )
        }}
    {% endif %}
    {% for provider, rule in providers.items() %}
        {% if (provider, rule) not in [
            ("native", "equal_points"),
            ("measureup", "binary_0_1"),
        ] %}
            {{
                exceptions.raise_compiler_error(
                    "Unsupported Quiz weakness scoring rule"
                )
            }}
        {% endif %}
    {% endfor %}
{% endfor %}
{% set config_json = tojson(rules) | replace("\\", "\\\\") | replace("'", "''") %}

with
    provider_items as (
        select
            i.*,
            i.item_catalog_status = 'verified'
            and notEmpty(ifNull(i.exam_subdomain_id, '')) as mapped
        from {{ ref("rpt_pss_quiz_item_results") }} as i
    ),
    -- The score models quarantine contradictory catalog ancestry and missing
    -- question IDs. Keep a marker for each attributable gap, without duplicating a
    -- surviving item.
    quarantined as (
        select distinct
            e.attempt_key as attempt_key,
            if(
                empty(ifNull(e.question_id, '')),
                concat('result:', e.source_result_id),
                concat('question:', e.question_id)
            ) as gap_key
        from {{ ref("int_pss_measureup_quiz_item_evidence") }} as e
        left anti join
            provider_items as i
            on e.attempt_key = i.attempt_key
            and e.question_id = i.provider_item_id
        where notEmpty(ifNull(e.attempt_key, '')) and e.linkage_status != 'linked'
    ),
    gaps as (
        select attempt_key, count() as gap_count from quarantined group by attempt_key
    ),
    attempts as (
        select *
        from {{ ref("rpt_pss_activity_attempts") }}
        where
            activity_type = 'Quiz'
            and provider = 'measureup'
            and ifNull(is_participation, 1) = 1
    ),
    -- An exported domain or subdomain can exist with no question results. Retain
    -- that branch even when other branches of the same sitting have answers.
    empty_branches as (
        select
            a.ccx_id as ccx_id,
            a.actor_id as actor_id,
            a.attempt_key as attempt_key,
            d.exam_domain_id as exam_domain_id,
            s.exam_subdomain_id as exam_subdomain_id,
            notEmpty(ifNull(dc.source_id, ''))
            and notEmpty(ifNull(sc.source_id, ''))
            and dc.exam_test_id = a.exam_test_id
            and sc.exam_domain_id = d.exam_domain_id as mapped
        from attempts as a
        inner join
            {{ ref("int_measureup_exam_attempt_domain_current") }} as d
            on d.exam_attempt_id = a.exam_attempt_id
        left join
            {{ ref("int_measureup_exam_attempt_subdomain_current") }} as s
            on s.attempt_domain_id = d.source_id
        left anti join
            {{ ref("int_measureup_exam_question_attempt_current") }} as q
            on q.subdomain_attempt_id = s.source_id
            and notEmpty(q.subdomain_attempt_id)
        left join
            {{ ref("int_measureup_exam_domain_current") }} as dc
            on dc.source_id = d.exam_domain_id
        left join
            {{ ref("int_measureup_exam_subdomain_current") }} as sc
            on sc.source_id = s.exam_subdomain_id
    ),
    provider_input as (
        select
            ccx_id,
            actor_id,
            attempt_key,
            exam_domain_id,
            exam_subdomain_id,
            mapped,
            item_key,
            result_status,
            item_outcome,
            provider_item_score
        from provider_items
        union all
        select
            ccx_id,
            actor_id,
            attempt_key,
            exam_domain_id,
            exam_subdomain_id,
            mapped,
            '',
            'empty_provider_branch',
            null,
            null
        from empty_branches
    ),
    provider as (
        select
            a.institution_id as institution_id,
            a.ccx_id as ccx_id,
            a.master_course as master_course,
            a.actor_id as actor_id,
            a.subsection_block_id as subsection_block_id,
            a.provider as provider,
            a.attempt_key as attempt_key,
            a.attempted_at as attempted_at,
            a.exam_test_id as assessment_version_id,
            a.version_status as version_status,
            if(
                a.version_status = 'verified', concat('measureup:', a.exam_test_id), ''
            ) as taxonomy_id,
            if(
                i.mapped and a.version_status = 'verified', i.exam_domain_id, null
            ) as domain_id,
            if(
                i.mapped and a.version_status = 'verified', i.exam_subdomain_id, null
            ) as subdomain_id,
            if(domain_id is not null, nullIf(d.name, ''), null) as domain_name,
            if(subdomain_id is not null, nullIf(s.name, ''), null) as subdomain_name,
            JSONExtractString(
                '{{ config_json }}', a.master_course, 'measureup'
            ) as scoring_rule,
            multiIf(
                a.is_participation is null,
                'eligibility_unverified',
                empty(ifNull(i.item_key, '')),
                'no_selected_items',
                a.version_status != 'verified',
                'attempt_version_unverified',
                i.result_status = 'within_attempt_selection_unapproved',
                'item_selection_unverified',
                not i.mapped,
                'objective_unverified',
                scoring_rule != 'binary_0_1',
                'rule_unapproved',
                i.result_status != 'provider_score_observed'
                or lowerUTF8(trim(ifNull(i.item_outcome, ''))) != 'answered'
                or i.provider_item_score is null
                or i.provider_item_score not in (0, 1),
                'item_outcome_unavailable',
                'scored'
            ) as item_status,
            nullIf(i.item_key, '') as item_key,
            if(item_status = 'scored', i.provider_item_score, null) as numerator,
            if(item_status = 'scored', toFloat64(1), null) as denominator,
            ifNull(g.gap_count, 0) as quarantined_items
        from attempts as a
        left join
            provider_input as i
            on i.attempt_key = a.attempt_key
            and i.ccx_id = a.ccx_id
            and i.actor_id = a.actor_id
        left join gaps as g on a.attempt_key = g.attempt_key
        left join
            {{ ref("int_measureup_exam_domain_current") }} as d
            on d.source_id = i.exam_domain_id
        left join
            {{ ref("int_measureup_exam_subdomain_current") }} as s
            on s.source_id = i.exam_subdomain_id
    ),
    native_items as (
        select
            *,
            if(
                mapping_status = 'mapped',
                labels,
                [tuple(toUInt64(0), toUInt64(0), toUInt64(0), '', '')]
            ) as selected_objectives
        from {{ ref("int_pss_native_quiz_item_attribution") }}
        where link_status = 'linked'
    ),
    native as (
        select
            institution_id,
            ccx_id,
            master_course,
            assumeNotNull(actor_id) as actor_id,
            subsection_block_id,
            'native' as provider,
            claimed_attempt_key as attempt_key,
            attempted_at,
            snapshot_id as assessment_version_id,
            'verified' as version_status,
            if(
                objective .1 = 0, '', concat('studio:', toString(objective .1))
            ) as taxonomy_id,
            nullIf(toString(objective .2), '0') as domain_id,
            nullIf(toString(objective .3), '0') as subdomain_id,
            nullIf(objective .4, '') as domain_name,
            nullIf(objective .5, '') as subdomain_name,
            JSONExtractString(
                '{{ config_json }}', master_course, 'native'
            ) as scoring_rule,
            multiIf(
                mapping_status != 'mapped',
                mapping_status,
                scoring_rule != 'equal_points',
                'rule_unapproved',
                earned_points is null
                or possible_points is null
                or possible_points <= 0
                or earned_points < 0
                or earned_points > possible_points,
                'item_outcome_unavailable',
                'scored'
            ) as item_status,
            concat('native:', assumeNotNull(problem_block_id)) as item_key,
            if(
                item_status = 'scored', earned_points / length(objective_keys), null
            ) as numerator,
            if(
                item_status = 'scored', possible_points / length(objective_keys), null
            ) as denominator,
            unlinked_claims as quarantined_items
        from native_items array
        join selected_objectives as objective
    )
select *
from native
union all
select *
from provider
