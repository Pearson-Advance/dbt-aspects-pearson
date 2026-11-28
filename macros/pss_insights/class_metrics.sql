{% macro pss_class_active_on(class_start, class_end, reporting_date) -%}
    {{ pss_context_active_on(class_start, class_end, reporting_date) }}
{%- endmacro %}

{% macro pss_class_metrics(
    classes,
    learners,
    practice_results,
    catalogs,
    delivery,
    reporting_date,
    practice_ranks,
    history_coverage,
    unlinked_attempts
) %}
    with
        certified_practice_members as (
            select distinct h.institution_id, h.ccx_id, h.actor_id, 1 as covered
            from {{ history_coverage }} as h
            inner join
                {{ catalogs }} as k
                on h.institution_id = k.institution_id
                and h.ccx_id = k.ccx_id
                and h.snapshot_id = k.snapshot_id
            where h.activity_type = 'Practice Exam' and h.full_history
        ),
        learner_totals as (
            select
                l.ccx_id as ccx_id,
                l.institution_id as institution_id,
                l.master_course as master_course,
                count() as observed_registered_learners,
                countIf(ifNull(h.covered, 0) = 0) as uncertified_practice_learners,
                countIf(
                    ifNull(r.ranking_status, '')
                    not in ('ranked', 'no_observed_practice', 'zero_eligible_items')
                    or (l.institution_id, l.ccx_id, l.actor_id) in (
                        select institution_id, ccx_id, actor_id
                        from {{ unlinked_attempts }}
                        where
                            empty(ifNull(activity_type, ''))
                            or activity_type = 'Practice Exam'
                    )
                ) as unresolved_practice_learners,
                countIf(l.readiness_status = 'On Target') as on_target,
                countIf(l.readiness_status = 'Borderline') as borderline,
                countIf(l.readiness_status = 'At Risk') as at_risk,
                avgOrNull(l.course_grade) as avg_course_grade,
                count(l.course_grade) as course_grade_learners,
                avgOrNull(l.content_progress) as avg_content_progress,
                count(l.content_progress) as content_progress_learners,
                toFloat64(sum(l.quiz_score_sum))
                / nullIf(sum(l.quiz_score_count), 0) as avg_quiz_score,
                toFloat64(sum(l.lab_score_sum))
                / nullIf(sum(l.lab_score_count), 0) as avg_lab_score,
                toFloat64(sum(l.practice_score_sum))
                / nullIf(sum(l.practice_score_count), 0) as avg_practice_exam_score,
                toFloat64(sum(l.final_score_sum))
                / nullIf(sum(l.final_score_count), 0) as avg_final_exam_score,
                -- Compare Decimal score sums to the exact threshold times their counts.
                -- Repeated 70% scores must never trigger support due to
                -- floating-point division.
                arrayFilter(
                    (activity, below) -> below,
                    ['Quiz', 'Lab', 'Practice Exam'],
                    [
                        {% for kind in ["quiz", "lab", "practice"] %}
                            sum(l.{{ kind }}_score_count) > 0
                            and sum(l.{{ kind }}_score_sum)
                            < toDecimal128('0.70', 2) * sum(l.{{ kind }}_score_count)
                            {% if not loop.last %},{% endif %}
                        {% endfor %}
                    ]
                ) as low_score_types,
                countIf(l.final_attempted = 1) as final_attempted_learners,
                countIf(l.final_passed = 1) as final_passed_learners,
                countIf(
                    l.final_attempted is null
                    or (l.final_attempted = 1 and l.final_passed is null)
                ) as final_unresolved_learners
            from {{ learners }} as l
            left join
                {{ practice_ranks }} as r
                on r.institution_id = l.institution_id
                and r.master_course = l.master_course
                and r.ccx_id = l.ccx_id
                and r.actor_id = l.actor_id
            left join
                certified_practice_members as h
                on h.institution_id = l.institution_id
                and h.ccx_id = l.ccx_id
                and h.actor_id = l.actor_id
            group by l.ccx_id, l.institution_id, l.master_course
        ),
        -- Pool item counts by provider identity before ranking. Learner rankings
        -- and averages of attempt-level percentages lose the item denominators.
        practice_totals as (
            select
                p.ccx_id as ccx_id,
                l.institution_id as institution_id,
                l.master_course as master_course,
                p.domain_id as domain_id,
                p.subdomain_id as subdomain_id,
                min(p.subdomain_name) as subdomain_name,
                sum(p.correct_items) as correct,
                sum(p.total_items) as total,
                correct / nullIf(total, 0) as score,
                countIf(
                    p.coverage_status != 'counted'
                    or (p.total_items > 0 and p.mapping_status != 'verified')
                ) as unresolved_buckets
            from {{ practice_results }} as p
            inner join
                {{ learners }} as l
                on p.institution_id = l.institution_id
                and p.master_course = l.master_course
                and p.ccx_id = l.ccx_id
                and p.actor_id = l.actor_id
            group by
                p.ccx_id, l.institution_id, l.master_course, p.domain_id, p.subdomain_id
        ),
        weakest as (
            select
                p.ccx_id as ccx_id,
                p.institution_id as institution_id,
                p.master_course as master_course,
                sum(p.unresolved_buckets) as unresolved_buckets,
                argMinIf(
                    tuple(
                        toNullable(p.domain_id),
                        toNullable(p.subdomain_id),
                        toNullable(p.subdomain_name),
                        p.score,
                        toNullable(p.correct),
                        toNullable(p.total)
                    ),
                    tuple(
                        p.score,
                        toUInt64OrNull(p.subdomain_id),
                        p.subdomain_id,
                        p.domain_id
                    ),
                    p.subdomain_id is not null
                    and p.domain_id is not null
                    and p.score is not null
                ) as result
            from practice_totals as p
            group by p.ccx_id, p.institution_id, p.master_course
        ),
        delivery_totals as (
            select
                d.ccx_id as ccx_id,
                d.institution_id as institution_id,
                d.master_course as master_course,
                count() as listed,
                count(d.delivery_complete) as evaluated,
                countIf(d.delivery_complete = 1) as completed
            from {{ delivery }} as d
            group by d.ccx_id, d.institution_id, d.master_course
        ),
        summaries as (
            select
                c.institution_id as institution_id,
                c.institution_ccx_id as institution_ccx_id,
                c.ccx_id as ccx_id,
                c.master_course as master_course,
                c.class_name as class_name,
                c.license_id as license_id,
                c.license_status as license_status,
                c.class_start as class_start,
                c.class_end as class_end,
                {{ reporting_date }} as reporting_date,
                {{
                    pss_class_active_on(
                        "c.class_start", "c.class_end", reporting_date
                    )
                }} as is_active,
                coalesce(nullIf(k.roster_status, ''), 'unavailable') as roster_status,
                coalesce(
                    l.observed_registered_learners, 0
                ) as observed_registered_learners,
                k.registered_source_learners as registered_source_learners,
                k.unresolved_registered_learners as unresolved_registered_learners,
                if(
                    k.registered_actor_denominator = observed_registered_learners,
                    k.registered_actor_denominator,
                    null
                ) as registered_learners,
                coalesce(l.on_target, 0) as on_target,
                coalesce(l.borderline, 0) as borderline,
                coalesce(l.at_risk, 0) as at_risk,
                on_target + borderline + at_risk as readiness_available,
                observed_registered_learners
                - readiness_available as readiness_unavailable,
                if(
                    registered_learners is not null,
                    on_target / nullIf(readiness_available, 0),
                    null
                ) as pct_on_target,
                if(
                    registered_learners is not null,
                    borderline / nullIf(readiness_available, 0),
                    null
                ) as pct_borderline,
                if(
                    registered_learners is not null,
                    at_risk / nullIf(readiness_available, 0),
                    null
                ) as pct_at_risk,
                l.avg_course_grade as avg_course_grade,
                coalesce(l.course_grade_learners, 0) as course_grade_learners,
                l.avg_content_progress as avg_content_progress,
                coalesce(l.content_progress_learners, 0) as content_progress_learners,
                l.avg_quiz_score as avg_quiz_score,
                l.avg_lab_score as avg_lab_score,
                l.avg_practice_exam_score as avg_practice_exam_score,
                l.avg_final_exam_score as avg_final_exam_score,
                l.low_score_types as low_score_types,
                multiIf(
                    registered_learners is null,
                    'roster_unverified',
                    registered_learners = 0,
                    'empty_roster',
                    catalog_status != 'complete',
                    'inventory_unavailable',
                    l.unresolved_practice_learners > 0 or w.unresolved_buckets > 0,
                    'eligibility_or_item_unavailable',
                    l.uncertified_practice_learners > 0,
                    'practice_history_unverified',
                    tupleElement(w.result, 4) is null,
                    'no_eligible_items',
                    'ranked'
                ) as weakest_practice_status,
                if(
                    weakest_practice_status = 'ranked', tupleElement(w.result, 1), null
                ) as weakest_practice_domain_id,
                if(
                    weakest_practice_status = 'ranked', tupleElement(w.result, 2), null
                ) as weakest_practice_subdomain_id,
                if(
                    weakest_practice_status = 'ranked', tupleElement(w.result, 3), null
                ) as weakest_practice_subdomain_name,
                if(
                    weakest_practice_status = 'ranked', tupleElement(w.result, 4), null
                ) as weakest_practice_subdomain_score,
                if(
                    weakest_practice_status = 'ranked', tupleElement(w.result, 5), null
                ) as weakest_practice_correct_items,
                if(
                    weakest_practice_status = 'ranked', tupleElement(w.result, 6), null
                ) as weakest_practice_total_items,
                coalesce(l.final_attempted_learners, 0) as final_attempted_learners,
                coalesce(l.final_passed_learners, 0) as final_passed_learners,
                coalesce(l.final_unresolved_learners, 0) as final_unresolved_learners,
                if(
                    registered_learners is not null and final_unresolved_learners = 0,
                    final_passed_learners / nullIf(final_attempted_learners, 0),
                    null
                ) as final_pass_rate,
                coalesce(nullIf(k.catalog_status, ''), 'unavailable') as catalog_status,
                coalesce(k.known_published_activities, 0) as known_published_activities,
                if(
                    catalog_status = 'complete', known_published_activities, null
                ) as total_activities,
                {{
                    pss_completed_activities(
                        "registered_learners",
                        "total_activities",
                        "d.listed",
                        "d.evaluated",
                        "d.completed",
                    )
                }} as completed_activities,
                total_activities - completed_activities as remaining_activities
            from {{ classes }} as c
            left join
                learner_totals as l
                on c.ccx_id = l.ccx_id
                and isNotDistinctFrom(c.institution_id, l.institution_id)
                and isNotDistinctFrom(c.master_course, l.master_course)
            left join
                weakest as w
                on c.ccx_id = w.ccx_id
                and isNotDistinctFrom(c.institution_id, w.institution_id)
                and isNotDistinctFrom(c.master_course, w.master_course)
            left join
                {{ catalogs }} as k
                on c.ccx_id = k.ccx_id
                and isNotDistinctFrom(c.institution_id, k.institution_id)
                and isNotDistinctFrom(c.master_course, k.master_course)
            left join
                delivery_totals as d
                on c.ccx_id = d.ccx_id
                and isNotDistinctFrom(c.institution_id, d.institution_id)
                and isNotDistinctFrom(c.master_course, d.master_course)
            where c.class_scope_status = 'in_scope'
        )
    select
        s.* except (low_score_types),
        multiIf(
            s.pct_on_target is null,
            null,
            s.pct_on_target >= 0.80,
            'Low',
            s.pct_on_target >= 0.70,
            'Medium',
            'High'
        ) as class_risk,
        if(
            class_risk is null, null, class_risk in ('Medium', 'High')
        ) as requires_attention,
        if(class_risk = 'High', s.low_score_types, []) as support_reasons,
        if(
            class_risk is null
            or (
                class_risk = 'High'
                and s.avg_quiz_score is null
                and s.avg_lab_score is null
                and s.avg_practice_exam_score is null
            ),
            null,
            notEmpty(support_reasons)
        ) as requires_instructor_support
    from summaries as s
{% endmacro %}
