{% macro pss_measureup_question_evidence(activity_type) %}
    -- Follow the same FK chain as rpt_measureup_question_performance. Starting at
    -- the tombstone-aware question results also retains otherwise orphaned rows
    -- for restricted reconciliation. A provider result is never joined by date.
    with
        question_catalog as (
            select
                q.source_id as question_id,
                nullIf(q.exam_subdomain_id, '') as subdomain_id,
                nullIf(s.exam_domain_id, '') as domain_id,
                nullIf(d.exam_test_id, '') as test_id
            from {{ ref("int_measureup_question_current") }} as q
            left join
                {{ ref("int_measureup_exam_subdomain_current") }} as s
                on q.exam_subdomain_id = s.source_id
            left join
                {{ ref("int_measureup_exam_domain_current") }} as d
                on s.exam_domain_id = d.source_id
        ),
        evidence as (
            select
                q.source_id as source_result_id,
                {% if activity_type == "Practice Exam" %}
                    q.subdomain_attempt_id as source_subdomain_attempt_id,
                {% endif %}
                q.question_id as question_id,
                nullIf(d.exam_attempt_id, '') as source_exam_attempt_id,
                nullIf(d.exam_domain_id, '') as exam_domain_id,
                nullIf(s.exam_subdomain_id, '') as exam_subdomain_id,
                nullIf(a.institution_id, '') as institution_id,
                nullIf(a.ccx_id, '') as ccx_id,
                nullIf(a.master_course, '') as master_course,
                nullIf(a.subsection_block_id, '') as subsection_block_id,
                nullIf(a.actor_id, '') as actor_id,
                nullIf(a.attempt_key, '') as attempt_key,
                nullIf(a.exam_test_id, '') as exam_test_id,
                nullIf(a.version_status, '') as version_status,
                if(
                    notEmpty(ifNull(a.attempt_key, '')), a.version_updated_at, null
                ) as version_updated_at,
                if(
                    notEmpty(ifNull(a.attempt_key, '')), a.attempted_at, null
                ) as attempted_at,
                if(
                    notEmpty(ifNull(a.attempt_key, '')), a.attempt_finished_at, null
                ) as attempt_finished_at,
                nullIf(q.status, '') as provider_status,
                nullIf(trimBoth(q.score), '') as source_score,
                toFloat64OrNull(source_score) as parsed_score,
                parseDateTime64BestEffortOrNull(
                    q.source_updated_at, 6, 'UTC'
                ) as source_updated_at,
                parseDateTime64BestEffortOrNull(
                    q.time_last_dumped, 6, 'UTC'
                ) as observed_at,
                multiIf(
                    c.subdomain_id != exam_subdomain_id
                    or c.domain_id != exam_domain_id
                    or c.test_id != exam_test_id,
                    'conflicting',
                    c.subdomain_id = exam_subdomain_id
                    and c.domain_id = exam_domain_id
                    and c.test_id = exam_test_id,
                    'verified',
                    'unavailable'
                ) as item_catalog_status,
                multiIf(
                    empty(trimBoth(q.source_id)),
                    'missing_result_id',
                    empty(ifNull(s.source_id, '')),
                    'missing_subdomain_attempt',
                    empty(ifNull(d.source_id, '')),
                    'missing_domain_attempt',
                    empty(ifNull(a.attempt_key, '')),
                    'parent_attempt_unresolved_or_outside_scope',
                    empty(ifNull(a.activity_type, '')),
                    'activity_type_at_start_unavailable',
                    empty(q.question_id),
                    'missing_question_id',
                    item_catalog_status = 'conflicting',
                    'item_catalog_conflict',
                    'linked'
                ) as linkage_status
            from {{ ref("int_measureup_exam_question_attempt_current") }} as q
            left join
                {{ ref("int_measureup_exam_attempt_subdomain_current") }} as s
                on q.subdomain_attempt_id = s.source_id
            left join
                {{ ref("int_measureup_exam_attempt_domain_current") }} as d
                on s.attempt_domain_id = d.source_id
            left join
                {{ ref("rpt_pss_activity_attempts") }} as a
                on d.exam_attempt_id = a.exam_attempt_id
                and a.provider = 'measureup'
            left join question_catalog as c on q.question_id = c.question_id
            where
                {% if activity_type == "Quiz" %}
                    empty(ifNull(a.activity_type, '')) or
                {% endif %}
                a.activity_type = '{{ activity_type }}'
        )
    select
        * except (parsed_score),
        if(isFinite(parsed_score), parsed_score, null) as provider_score,
        count() over (
            partition by
                attempt_key,
                question_id,
                if(
                    linkage_status in ('linked', 'item_catalog_conflict'),
                    '',
                    source_result_id
                )
        ) as source_result_count
    from evidence
{% endmacro %}
