{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(coalesce(institution_id, ''), ccx_id, actor_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": pss_reporting_upstream_mvs("learner"),
        },
        tags=["pss_insights"],
    )
}}

with
    grade_updates as (
        -- The LMS grade already includes the LMS treatment of unattempted work.
        select
            course_key as ccx_id,
            actor_id,
            emission_time,
            if(
                uniqExact(tuple(course_grade)) = 1,
                any(tuple(toFloat64(course_grade))) .1,
                null
            ) as reported_grade
        from {{ ref("dim_learner_most_recent_course_grade") }}
        where course_key in (select ccx_id from {{ ref("int_pss_class_context") }})
        group by course_key, actor_id, emission_time
    ),
    grades as (
        select
            ccx_id,
            actor_id,
            argMax(tuple(reported_grade), emission_time) .1 as course_grade
        from grade_updates
        group by ccx_id, actor_id
    ),
    current_scores as (
        select
            ccx_id,
            actor_id,
            sumIf(score_pct / 100.0, activity_type = 'Quiz') as quiz_score_sum,
            countIf(activity_type = 'Quiz') as quiz_score_count,
            sumIf(score_pct / 100.0, activity_type = 'Lab') as lab_score_sum,
            countIf(activity_type = 'Lab') as lab_score_count
        from {{ ref("rpt_pss_activity_scores_current") }}
        where activity_type in ('Quiz', 'Lab') and score_pct between 0 and 100
        group by ccx_id, actor_id
    )
select
    c.institution_id as institution_id,
    c.master_course as master_course,
    r.ccx_id as ccx_id,
    r.actor_id as actor_id,
    g.course_grade as course_grade,
    n.published_unit_count as published_unit_count,
    n.observed_visited_unit_count as observed_visited_unit_count,
    n.visited_unit_count as visited_unit_count,
    n.content_progress as content_progress,
    nullIf(rd.readiness_status, '') as readiness_status,
    if(
        nullIf(rd.readiness_status, '') is null,
        coalesce(nullIf(rd.readiness_unavailable_reason, ''), 'readiness_unavailable'),
        null
    ) as readiness_unavailable_reason,
    coalesce(s.quiz_score_sum, 0) as quiz_score_sum,
    coalesce(s.quiz_score_count, 0) as quiz_score_count,
    coalesce(s.lab_score_sum, 0) as lab_score_sum,
    coalesce(s.lab_score_count, 0) as lab_score_count,
    coalesce(a.practice_score_sum, 0) as practice_score_sum,
    coalesce(a.practice_score_count, 0) as practice_score_count,
    coalesce(a.final_score_sum, 0) as final_score_sum,
    coalesce(a.final_score_count, 0) as final_score_count,
    coalesce(a.qualifying_final_attempts, 0) as qualifying_final_attempts,
    coalesce(a.final_unresolved_attempts, 0) as final_unresolved_attempts,
    multiIf(
        a.qualifying_final_attempts > 0, 1, cov.final_attempts_complete = 1, 0, null
    ) as final_attempted,
    multiIf(
        a.passed_final_attempts > 0,
        1,
        coalesce(cov.final_attempts_complete, 0) != 1
        or coalesce(a.qualifying_final_attempts, 0) = 0
        or a.final_unresolved_attempts > 0,
        null,
        0
    ) as final_passed
from {{ ref("int_pss_reporting_roster") }} as r
inner join {{ ref("int_pss_class_context") }} as c on r.ccx_id = c.ccx_id
left join grades as g on r.ccx_id = g.ccx_id and r.actor_id = g.actor_id
left join current_scores as s on r.ccx_id = s.ccx_id and r.actor_id = s.actor_id
left join
    {{ ref("int_pss_learner_attempt_summary") }} as a
    on r.ccx_id = a.ccx_id
    and r.actor_id = a.actor_id
left join
    {{ ref("rpt_pss_learner_readiness_current") }} as rd
    on r.ccx_id = rd.ccx_id
    and r.actor_id = rd.actor_id
left join
    {{ ref("int_pss_learner_navigation") }} as n
    on r.ccx_id = n.ccx_id
    and r.actor_id = n.actor_id
left join {{ ref("int_pss_reporting_class_coverage") }} as cov on r.ccx_id = cov.ccx_id
