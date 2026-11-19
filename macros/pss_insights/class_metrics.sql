{% macro pss_class_active_on(class_start, class_end, reporting_date) -%}
    if(
        {{ class_start }} is null or {{ class_end }} is null,
        null,
        {{ reporting_date }} between {{ class_start }} and {{ class_end }}
    )
{%- endmacro %}

{% macro pss_class_metrics(
    classes, learners, practice_results, catalogs, delivery, reporting_date
) %}
    with
        learner_totals as (
            select
                l.ccx_id as ccx_id,
                l.institution_id as institution_id,
                l.master_course as master_course,
                count() as registered_learners,
                countIf(l.readiness_status = 'On Target') as on_target,
                countIf(l.readiness_status = 'Borderline') as borderline,
                countIf(l.readiness_status = 'At Risk') as at_risk,
                avgOrNull(l.course_grade) as avg_course_grade,
                count(l.course_grade) as course_grade_learners,
                avgOrNull(l.content_progress) as avg_content_progress,
                count(l.content_progress) as content_progress_learners,
                sum(l.quiz_score_sum)
                / nullIf(sum(l.quiz_score_count), 0) as avg_quiz_score,
                sum(l.lab_score_sum)
                / nullIf(sum(l.lab_score_count), 0) as avg_lab_score,
                sum(l.practice_score_sum)
                / nullIf(sum(l.practice_score_count), 0) as avg_practice_exam_score,
                sum(l.final_score_sum)
                / nullIf(sum(l.final_score_count), 0) as avg_final_exam_score,
                countIf(l.final_attempted = 1) as final_attempted_learners,
                countIf(l.final_passed = 1) as final_passed_learners,
                countIf(
                    l.final_attempted is null
                    or (l.final_attempted = 1 and l.final_passed is null)
                ) as final_unresolved_learners
            from {{ learners }} as l
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
                sum(p.correct_items) / nullIf(sum(p.total_items), 0) as score
            from {{ practice_results }} as p
            inner join
                {{ learners }} as l on p.ccx_id = l.ccx_id and p.actor_id = l.actor_id
            where
                p.total_items > 0
                and p.correct_items between 0 and p.total_items
                and p.domain_id is not null
                and p.subdomain_id is not null
            group by
                p.ccx_id, l.institution_id, l.master_course, p.domain_id, p.subdomain_id
        ),
        weakest as (
            select
                p.ccx_id as ccx_id,
                p.institution_id as institution_id,
                p.master_course as master_course,
                argMin(
                    tuple(
                        toNullable(p.domain_id),
                        toNullable(p.subdomain_id),
                        toNullable(p.subdomain_name),
                        p.score
                    ),
                    tuple(p.score, p.domain_id, p.subdomain_id)
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
                coalesce(l.registered_learners, 0) as registered_learners,
                coalesce(l.on_target, 0) as on_target,
                coalesce(l.borderline, 0) as borderline,
                coalesce(l.at_risk, 0) as at_risk,
                on_target + borderline + at_risk as readiness_available,
                registered_learners - readiness_available as readiness_unavailable,
                on_target / nullIf(readiness_available, 0) as pct_on_target,
                borderline / nullIf(readiness_available, 0) as pct_borderline,
                at_risk / nullIf(readiness_available, 0) as pct_at_risk,
                l.avg_course_grade as avg_course_grade,
                coalesce(l.course_grade_learners, 0) as course_grade_learners,
                l.avg_content_progress as avg_content_progress,
                coalesce(l.content_progress_learners, 0) as content_progress_learners,
                l.avg_quiz_score as avg_quiz_score,
                l.avg_lab_score as avg_lab_score,
                l.avg_practice_exam_score as avg_practice_exam_score,
                l.avg_final_exam_score as avg_final_exam_score,
                tupleElement(w.result, 1) as weakest_practice_domain_id,
                tupleElement(w.result, 2) as weakest_practice_subdomain_id,
                tupleElement(w.result, 3) as weakest_practice_subdomain_name,
                tupleElement(w.result, 4) as weakest_practice_subdomain_score,
                coalesce(l.final_attempted_learners, 0) as final_attempted_learners,
                coalesce(l.final_passed_learners, 0) as final_passed_learners,
                coalesce(l.final_unresolved_learners, 0) as final_unresolved_learners,
                if(
                    final_unresolved_learners = 0,
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
        )
    select
        s.*,
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
        arrayFilter(
            (activity, score) -> class_risk = 'High' and round(score, 12) < 0.70,
            ['Quiz', 'Lab', 'Practice Exam'],
            [s.avg_quiz_score, s.avg_lab_score, s.avg_practice_exam_score]
        ) as support_reasons,
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
