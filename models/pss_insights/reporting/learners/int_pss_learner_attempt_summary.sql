{{ config(tags=["pss_insights"]) }}

with
    finals_policy as (
        select master_course, final_exam_pass_rule, final_exam_pass_mark
        from {{ ref("int_pss_grading_policy") }}
        where activity_type = 'Final Exam'
    ),
    eligible as (
        select
            a.ccx_id as ccx_id,
            a.actor_id as actor_id,
            a.activity_type = 'Final Exam' as is_final,
            if(a.score_pct between 0 and 100, a.score_pct, null) as score_pct,
            a.is_passed as provider_passed,
            p.final_exam_pass_rule as pass_rule,
            multiIf(
                p.final_exam_pass_rule = 'native_pass_mark' and a.provider = 'native',
                p.final_exam_pass_mark,
                p.final_exam_pass_rule = 'measureup_cut_score'
                and a.provider = 'measureup',
                a.cut_score,
                null
            ) as pass_mark
        from {{ ref("rpt_pss_activity_attempts") }} as a
        inner join {{ ref("int_pss_class_context") }} as c on a.ccx_id = c.ccx_id
        left join finals_policy as p on c.master_course = p.master_course
        where
            (a.activity_type = 'Final Exam' and a.is_qualifying_final)
            or (a.activity_type = 'Practice Exam' and a.is_eligible_practice)
    ),
    adjudicated as (
        select
            *,
            if(
                pass_mark between 0 and 100, score_pct >= pass_mark, null
            ) as policy_passed,
            if(
                pass_rule = 'measureup_cut_score'
                and provider_passed is not null
                and provider_passed != policy_passed,
                null,
                policy_passed
            ) as passed
        from eligible
    )
select
    ccx_id,
    actor_id,
    countIf(is_final) as qualifying_final_attempts,
    countIf(is_final and passed = 1) as passed_final_attempts,
    countIf(is_final and passed is null) as final_unresolved_attempts,
    coalesce(sumIf(score_pct / 100.0, is_final), 0) as final_score_sum,
    countIf(is_final and score_pct is not null) as final_score_count,
    coalesce(sumIf(score_pct / 100.0, not is_final), 0) as practice_score_sum,
    countIf(not is_final and score_pct is not null) as practice_score_count
from adjudicated
group by ccx_id, actor_id
