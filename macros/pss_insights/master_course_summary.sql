{# authorized_classes is a relation/query filtered for the requesting principal.
   The two-column semi-join happens before learner deduplication. #}
{% macro pss_master_course_summary(
    authorized_classes,
    reporting_date,
    class_metrics=none,
    learner_progress=none
) %}
    with
        active_classes as (
            select institution_id, master_course, ccx_id, class_risk
            from {{ class_metrics or ref("rpt_pss_class_metrics") }}
            where
                notEmpty(ifNull(institution_id, ''))
                and (institution_id, ccx_id)
                in (select institution_id, ccx_id from {{ authorized_classes }})
                and {{ pss_class_active_on("class_start", "class_end", reporting_date) }}
                and notEmpty(ifNull(master_course, ''))
        ),
        classes as (
            select
                master_course,
                count() as active_classes,
                countIf(
                    class_risk in ('Medium', 'High')
                ) as classes_requiring_attention,
                countIf(class_risk is null) as classes_with_unavailable_risk
            from active_classes
            group by master_course
        ),
        learners as (
            select
                active.master_course as master_course,
                progress.actor_id as actor_id,
                -- A shared learner with conflicting or missing bands needs review.
                if(
                    countIf(progress.readiness_status is null) = 0
                    and uniqExact(progress.readiness_status) = 1,
                    any(progress.readiness_status),
                    null
                ) as band,
                multiIf(
                    countIf(progress.final_attempted = 1) > 0,
                    1,
                    countIf(progress.final_attempted is null) > 0,
                    null,
                    0
                ) as attempted,
                multiIf(
                    countIf(progress.final_passed = 1) > 0,
                    1,
                    countIf(
                        progress.final_attempted is null
                        or (
                            progress.final_attempted = 1
                            and progress.final_passed is null
                        )
                    )
                    > 0,
                    null,
                    0
                ) as passed
            from {{ learner_progress or ref("rpt_pss_learner_progress") }} as progress
            inner join
                active_classes as active
                on progress.institution_id = active.institution_id
                and progress.ccx_id = active.ccx_id
                and progress.master_course = active.master_course
            group by active.master_course, progress.actor_id
        ),
        counts as (
            select
                master_course,
                count() as registered_learners,
                countIf(band = 'On Target') as on_target,
                countIf(band = 'Borderline') as borderline,
                countIf(band = 'At Risk') as at_risk,
                countIf(attempted = 1) as final_attempted_learners,
                countIf(passed = 1) as final_passed_learners,
                countIf(
                    attempted is null or (attempted = 1 and passed is null)
                ) as final_unresolved_learners
            from learners
            group by master_course
        )
    select
        classes.master_course as master_course,
        classes.active_classes as active_classes,
        classes.classes_requiring_attention as classes_requiring_attention,
        classes.classes_with_unavailable_risk as classes_with_unavailable_risk,
        coalesce(counts.registered_learners, 0) as registered_learners,
        coalesce(counts.on_target, 0) as on_target,
        coalesce(counts.borderline, 0) as borderline,
        coalesce(counts.at_risk, 0) as at_risk,
        on_target + borderline + at_risk as readiness_available,
        registered_learners - readiness_available as readiness_unavailable,
        on_target / nullIf(readiness_available, 0) as pct_on_target,
        borderline / nullIf(readiness_available, 0) as pct_borderline,
        at_risk / nullIf(readiness_available, 0) as pct_at_risk,
        coalesce(counts.final_attempted_learners, 0) as final_attempted_learners,
        coalesce(counts.final_passed_learners, 0) as final_passed_learners,
        coalesce(counts.final_unresolved_learners, 0) as final_unresolved_learners,
        if(
            final_unresolved_learners = 0,
            final_passed_learners / nullIf(final_attempted_learners, 0),
            null
        ) as final_pass_rate
    from classes
    left join counts on counts.master_course = classes.master_course
{% endmacro %}
