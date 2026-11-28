{{ config(schema="pss_restricted", tags=["pss_readiness"]) }}

{% set rules = var("pss_practice_item_rules", {}) %}
{% if rules is not mapping %}
    {{
        exceptions.raise_compiler_error(
            "pss_practice_item_rules must map each master run to its approved denominator rule"
        )
    }}
{% endif %}
{% for master, rule in rules.items() %}
    {% if master is not string or not master.startswith(
        "course-v1:"
    ) or rule not in ["scored_only", "all_exported"] %}
        {{
            exceptions.raise_compiler_error(
                "Unknown PSS practice course key or item rule"
            )
        }}
    {% endif %}
{% endfor %}
{% set encoded = tojson(rules) | replace("\\", "\\\\") | replace("'", "''") %}

-- Start with eligible sittings and their subdomain-attempt records: a missing
-- domain, subdomain or set of questions must leave visible coverage evidence.
with
    raw as (
        select
            a.institution_id as institution_id,
            a.master_course as master_course,
            a.ccx_id as ccx_id,
            a.actor_id as actor_id,
            a.subsection_block_id as subsection_block_id,
            a.attempt_key as attempt_key,
            a.exam_attempt_id as exam_attempt_id,
            a.exam_test_id as exam_test_id,
            a.attempted_at as attempted_at,
            a.version_status as version_status,
            a.is_finished as is_finished,
            coalesce(
                nullIf(JSONExtractString('{{ encoded }}', a.master_course), ''),
                'binary_only'
            ) as item_rule,
            q.source_result_id as source_result_id,
            q.question_id as question_id,
            q.linkage_status as linkage_status,
            q.source_result_count as source_result_count,
            q.source_score as source_score,
            q.provider_score as observed_score,
            lowerUTF8(trimBoth(ifNull(q.provider_status, ''))) as provider_status,
            nullIf(d.exam_domain_id, '') as source_domain_id,
            nullIf(s.exam_subdomain_id, '') as source_subdomain_id,
            nullIf(dc.source_id, '') as catalog_domain,
            nullIf(sc.source_id, '') as catalog_subdomain,
            dc.exam_test_id as domain_test,
            sc.exam_domain_id as subdomain_parent,
            nullIf(dc.name, '') as domain_name,
            nullIf(sc.name, '') as subdomain_name,
            q.item_catalog_status as item_catalog_status,
            multiIf(
                catalog_domain is not null
                and notEmpty(domain_test)
                and domain_test != a.exam_test_id,
                'conflicting',
                catalog_subdomain is not null
                and notEmpty(subdomain_parent)
                and subdomain_parent != source_domain_id,
                'conflicting',
                catalog_domain is not null
                and catalog_subdomain is not null
                and domain_test = a.exam_test_id
                and subdomain_parent = source_domain_id,
                'verified',
                'unavailable'
            ) as branch_status,
            -- A missing question catalog does not erase a unique observed answer.
            -- Only verified, consistent objective identities enter the ranking.
            a.version_status = 'verified'
            and branch_status = 'verified'
            and (
                empty(ifNull(source_result_id, '')) or item_catalog_status = 'verified'
            ) as objective_verified
        from {{ ref("rpt_pss_activity_attempts") }} as a
        left join
            {{ ref("int_measureup_exam_attempt_domain_current") }} as d
            on d.exam_attempt_id = a.exam_attempt_id
        left join
            {{ ref("int_measureup_exam_attempt_subdomain_current") }} as s
            on d.source_id = s.attempt_domain_id
        left join
            {{ ref("int_pss_practice_question_evidence") }} as q
            on q.source_subdomain_attempt_id = s.source_id
            and a.attempt_key = q.attempt_key
        left join
            {{ ref("int_measureup_exam_domain_current") }} as dc
            on d.exam_domain_id = dc.source_id
        left join
            {{ ref("int_measureup_exam_subdomain_current") }} as sc
            on s.exam_subdomain_id = sc.source_id
        where
            a.provider = 'measureup'
            and a.activity_type = 'Practice Exam'
            and a.is_eligible_practice = 1
    )
select
    institution_id,
    master_course,
    ccx_id,
    actor_id,
    subsection_block_id,
    attempt_key,
    exam_attempt_id,
    exam_test_id,
    attempted_at,
    version_status,
    item_rule,
    nullIf(source_result_id, '') as result_id,
    nullIf(question_id, '') as item_id,
    if(objective_verified, domain_name, null) as objective_domain_name,
    if(objective_verified, subdomain_name, null) as objective_subdomain_name,
    if(objective_verified, source_domain_id, null) as domain_id,
    if(objective_verified, source_subdomain_id, null) as subdomain_id,
    multiIf(
        empty(ifNull(source_result_id, '')),
        'missing_item_evidence',
        source_result_count != 1
        or linkage_status != 'linked'
        or provider_status not in ('answered', 'unanswered')
        or branch_status = 'conflicting',
        'unresolved_item',
        not is_finished and item_rule = 'binary_only',
        'item_policy_required',
        (
            source_score is not null
            and (observed_score is null or observed_score < 0 or observed_score > 1)
        )
        or (provider_status = 'unanswered' and observed_score > 0),
        'invalid_score',
        isNull(source_score) or provider_status = 'unanswered',
        multiIf(
            item_rule = 'all_exported',
            'incorrect',
            item_rule = 'scored_only',
            'excluded',
            'item_policy_required'
        ),
        observed_score = 1,
        'correct',
        observed_score = 0,
        'incorrect',
        item_rule in ('all_exported', 'scored_only'),
        'incorrect',
        'item_policy_required'
    ) as item_outcome
from raw
